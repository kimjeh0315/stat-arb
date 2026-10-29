#!/usr/bin/env ruby
# Count current S3 objects in a UTC date partition without downloading data.
require 'date'
require 'json'
require 'open3'
require 'fileutils'
require 'rexml/document'
require 'rexml/xpath'

ROOT = File.expand_path('..', __dir__)
DAY = ARGV.fetch(0, '2026-10-25')
abort 'Date must be YYYY-MM-DD' unless DAY.match?(/\A\d{4}-\d{2}-\d{2}\z/) && Date.iso8601(DAY).to_s == DAY
env = ENV.to_h
File.foreach(ENV.fetch('ENV_FILE', File.join(ROOT, '.env'))) do |line|
  match = line.match(/\A\s*(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AWS_REGION|QUANT_S3_BUCKET|QUANT_S3_PREFIX)\s*=(.*?)\s*\z/)
  next unless match
  value = match[2]
  value = value[1...-1] if (value.start_with?('"') && value.end_with?('"')) || (value.start_with?("'") && value.end_with?("'"))
  env[match[1]] = value
end
%w[AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION QUANT_S3_BUCKET].each do |key|
  abort "Missing variable: #{key}" if env.fetch(key, '').empty?
end
abort 'Invalid region' unless env['AWS_REGION'].match?(/\A[a-z0-9-]+\z/)
abort 'Invalid bucket' unless env['QUANT_S3_BUCKET'].match?(/\A[a-z0-9][a-z0-9.-]+\z/)
credential_config = "user = \"#{env['AWS_ACCESS_KEY_ID']}:#{env['AWS_SECRET_ACCESS_KEY']}\"\n"
credential_config += "header = \"x-amz-security-token: #{env['AWS_SESSION_TOKEN']}\"\n" unless env.fetch('AWS_SESSION_TOKEN', '').empty?
base = "https://s3.#{env['AWS_REGION']}.amazonaws.com/#{env['QUANT_S3_BUCKET']}"
root_prefix = env.fetch('QUANT_S3_PREFIX', 'quant').sub(%r{\A/+}, '').sub(%r{/+\z}, '') + '/'
request_count = 0
request_mutex = Mutex.new
fetch_page = lambda do |prefix, delimiter, continuation|
  params = {'list-type' => '2', 'max-keys' => '1000', 'prefix' => prefix}
  params['delimiter'] = '/' if delimiter
  params['continuation-token'] = continuation if continuation
  command = ['curl', '--config', '-', '--aws-sigv4', "aws:amz:#{env['AWS_REGION']}:s3",
             '--silent', '--show-error', '--connect-timeout', '15', '--max-time', '120',
             '--write-out', "\n%{http_code}", '--get', base]
  params.each { |k, v| command.concat(['--data-urlencode', "#{k}=#{v}"]) }
  body = nil
  status = nil
  3.times do |attempt|
    body, _stderr, status = Open3.capture3(*command, :stdin_data => credential_config)
    break if status.success? && !body.match?(/\n(?:429|500|502|503|504)\z/)
    warn "Retrying S3 metadata request (attempt #{attempt + 2}/3)" if attempt < 2
    sleep(1) if attempt < 2
  end
  raise "S3 network request failed (curl exit #{status.exitstatus}; credentials are not logged)" unless status.success?
  xml, _, http_status = body.rpartition("\n")
  doc = REXML::Document.new(xml)
  if http_status != '200'
    code = REXML::XPath.first(doc, '//*[local-name()="Error"]/*[local-name()="Code"]')
    raise "S3 listing failed: HTTP #{http_status} (#{code && code.text})"
  end
  request_mutex.synchronize { request_count += 1 }
  doc
end
each_page = lambda do |prefix, delimiter, &block|
  token = nil
  loop do
    doc = fetch_page.call(prefix, delimiter, token)
    block.call(doc)
    truncated = REXML::XPath.first(doc, '/*/*[local-name()="IsTruncated"]')
    break unless truncated && truncated.text == 'true'
    next_token = REXML::XPath.first(doc, '/*/*[local-name()="NextContinuationToken"]')
    raise 'Missing or repeated S3 pagination token' unless next_token && next_token.text && next_token.text != token
    token = next_token.text
  end
end
puts "Discovering UTC date=#{DAY} partitions (object contents will not be downloaded)..."
targets = []
visit = lambda do |prefix|
  if prefix.match?(%r{(?:\A|/)date=[^/]+/})
    targets << prefix if prefix.include?("/date=#{DAY}/")
    return
  end
  each_page.call(prefix, true) do |doc|
    REXML::XPath.each(doc, '/*/*[local-name()="CommonPrefixes"]/*[local-name()="Prefix"]') { |p| visit.call(p.text) }
  end
end
visit.call(root_prefix)
puts "Found #{targets.size} date partitions; counting all pages..."
jobs = Queue.new
targets.each { |prefix| jobs << prefix }
rows = []
errors = []
result_mutex = Mutex.new
workers = Array.new(4) do
  Thread.new do
    loop do
      begin
        prefix = jobs.pop(true)
      rescue ThreadError
        break
      end
      begin
        count = 0
        bytes = 0
        each_page.call(prefix, false) do |doc|
          REXML::XPath.each(doc, '/*/*[local-name()="Contents"]') do |object|
            key = object.elements['Key'].text
            next if key.end_with?('/')
            raise 'Unexpected date in listing' unless key.start_with?(prefix)
            count += 1
            bytes += Integer(object.elements['Size'].text)
          end
        end
        relative = prefix.delete_prefix(root_prefix)
        row = { :layer => relative.split('/').first, :dataset => relative[/dataset=([^\/]+)/, 1] || 'raw',
                :exchange => relative[/exchange=([^\/]+)/, 1] || 'unknown', :objects => count, :bytes => bytes }
        result_mutex.synchronize do
          rows << row
          puts "Counted #{rows.size}/#{targets.size} partitions" if rows.size % 8 == 0 || rows.size == targets.size
        end
      rescue StandardError => e
        result_mutex.synchronize { errors << e.message }
      end
    end
  end
end
workers.each(&:join)
abort "Incomplete report: #{errors.uniq.join('; ')}" unless errors.empty?
rows.sort_by! { |r| [r[:layer], r[:dataset], r[:exchange]] }
report = { :utc_date => DAY, :scope => root_prefix, :objects => rows.sum { |r| r[:objects] },
           :bytes => rows.sum { |r| r[:bytes] }, :list_requests => request_count, :partitions => rows }
report[:by_layer] = rows.group_by { |r| r[:layer] }.transform_values { |group| { :objects => group.sum { |r| r[:objects] }, :bytes => group.sum { |r| r[:bytes] } } }
report[:by_exchange] = rows.group_by { |r| r[:exchange] }.transform_values { |group| { :objects => group.sum { |r| r[:objects] }, :bytes => group.sum { |r| r[:bytes] } } }
out_dir = File.join(ROOT, 'data', 'reports')
FileUtils.mkdir_p(out_dir)
output = File.join(out_dir, "s3-size-#{DAY}.json")
File.write(output, JSON.pretty_generate(report) + "\n", :mode => 'w', :perm => 0600)
puts JSON.pretty_generate(report.reject { |k, _| k == :partitions })
puts "Report: #{output}"
