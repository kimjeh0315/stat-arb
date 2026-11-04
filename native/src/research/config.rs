use super::Args;
use anyhow::{Context, Result, bail, ensure};
use clap::{Command, CommandFactory, FromArgMatches};
use serde_json::Value;

// File values become clap defaults, so explicit command-line values (including
// whole arrays) replace them. All type checking remains in the existing parser.
fn command(value: &Value) -> Result<Command> {
    let fields = value.as_object().context("config must be a JSON object")?;
    let mut command = Args::command();
    for (name, value) in fields {
        ensure!(
            !["config", "print_config", "exchanges"].contains(&name.as_str())
                && command
                    .get_arguments()
                    .any(|arg| arg.get_id().as_str() == name),
            "unknown config parameter: {name} (use snake_case names)"
        );
        // null means use the built-in default / unset an optional parameter.
        if value.is_null() {
            continue;
        }
        let is_list = ["assets", "windows_ms", "entry_z", "exit_z"].contains(&name.as_str());
        ensure!(
            is_list == value.is_array(),
            "{name}: expected {}",
            if is_list { "an array" } else { "a scalar" }
        );
        let values = if let Value::Array(values) = value {
            values.clone()
        } else {
            vec![value.clone()]
        };
        ensure!(!values.is_empty(), "{name}: array must not be empty");
        let values: Vec<String> = values
            .iter()
            .map(|value| match value {
                Value::String(s) => Ok(s.clone()),
                Value::Number(n) => Ok(n.to_string()),
                _ => bail!("{name}: expected string or number"),
            })
            .collect::<Result<_>>()?;
        command = command.mut_arg(name, |arg| arg.required(false).default_values(values));
    }
    Ok(command)
}

pub fn load() -> Result<Args> {
    let raw: Vec<_> = std::env::args_os().collect();
    // Discover the file without requiring options supplied inside it. clap still
    // handles --help, unknown options, and explicit value syntax normally.
    let initial = Args::command()
        .mut_args(|arg| arg.required(false))
        .get_matches_from(&raw);
    let command = if let Some(path) = initial.get_one::<std::path::PathBuf>("config") {
        let bytes = std::fs::read(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let value: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid JSON in {}", path.display()))?;
        command(&value)?
    } else {
        Args::command()
    };
    Ok(Args::from_arg_matches(&command.get_matches_from(raw))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn example() -> Value {
        serde_json::from_str(include_str!("../../../config/zscore-oct20-30.json")).unwrap()
    }
    #[test]
    fn example_supplies_required_options_and_defaults() {
        let matches = command(&example())
            .unwrap()
            .try_get_matches_from(["zscore"])
            .unwrap();
        let args = Args::from_arg_matches(&matches).unwrap();
        assert_eq!(args.start.to_rfc3339(), "2026-10-20T00:00:00+00:00");
        assert_eq!(args.end.to_rfc3339(), "2026-10-30T00:00:00+00:00");
        assert_eq!(args.sample_ms, 100);
        assert_eq!(args.windows_ms, vec![5000, 10000, 30000]);
        assert!(args.max_files.is_none());
        assert_eq!(args.assets, vec!["BTC", "ETH", "SOL", "XRP", "DOGE"]);
        assert_eq!(super::super::targets(&args).unwrap().len(), 5);
    }
    #[test]
    fn cli_overrides_file_including_entire_array() {
        let matches = command(&example())
            .unwrap()
            .try_get_matches_from([
                "zscore",
                "--windows-ms",
                "2000,4000",
                "--entry-z",
                "3",
                "--sample-ms",
                "50",
                "--output",
                "different-output",
            ])
            .unwrap();
        let args = Args::from_arg_matches(&matches).unwrap();
        assert_eq!(args.windows_ms, vec![2000, 4000]);
        assert_eq!(args.entry_z, vec![3.0]);
        assert_eq!(args.sample_ms, 50);
        assert_eq!(args.output, std::path::PathBuf::from("different-output"));
    }
    #[test]
    fn rejects_typos_wrong_shapes_and_invalid_values() {
        for value in [
            serde_json::json!({"entry_zz": [2]}),
            serde_json::json!({"entry_z": []}),
            serde_json::json!({"entry_z": 2}),
            serde_json::json!({"sample_ms": [100]}),
            serde_json::json!({"config":"nested.json"}),
        ] {
            assert!(command(&value).is_err());
        }
        let mut value = example();
        value["sample_ms"] = serde_json::json!("invalid");
        assert!(
            command(&value)
                .unwrap()
                .try_get_matches_from(["zscore"])
                .is_err()
        );
    }
    #[test]
    fn missing_required_fields_still_fail() {
        assert!(
            command(&serde_json::json!({}))
                .unwrap()
                .try_get_matches_from(["zscore"])
                .is_err()
        );
    }
}
