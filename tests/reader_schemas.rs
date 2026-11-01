//! JSON Schemas of `read_session` are generated from the Rust wire types. Regenerate with
//! `UPDATE_SCHEMAS=1 cargo test --test reader_schemas`; otherwise the committed files must match.
//! Example exchanges under `examples/read_session/` are validated against them.

use rmcp::schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::Value;
use spool::transcript::types::{ReadSessionError, ReadSessionInput, ReadSessionOutput};
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn generate<T: JsonSchema + 'static>() -> Value {
    let schema = SchemaSettings::draft2020_12()
        .into_generator()
        .into_root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema serializes")
}

fn files() -> Vec<(&'static str, Value)> {
    vec![
        ("input", generate::<ReadSessionInput>()),
        ("output", generate::<ReadSessionOutput>()),
        ("error", generate::<ReadSessionError>()),
    ]
}

#[test]
fn schema_files_match_the_rust_types() {
    let dir = root().join("schemas/read_session");
    let update = std::env::var("UPDATE_SCHEMAS").is_ok_and(|v| v == "1");
    for (name, schema) in files() {
        let path = dir.join(format!("{name}.schema.json"));
        if update {
            std::fs::create_dir_all(&dir).unwrap();
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, serde_json::to_string_pretty(&schema).unwrap() + "\n").unwrap();
            #[cfg(windows)]
            std::fs::remove_file(&path).unwrap();
            std::fs::rename(&tmp, &path).unwrap(); // replace only after writing the complete schema
        }
        let on_disk: Value = serde_json::from_slice(
            &std::fs::read(&path)
                .unwrap_or_else(|e| panic!("{}: {e}; run UPDATE_SCHEMAS=1", path.display())),
        )
        .unwrap();
        assert_eq!(
            on_disk, schema,
            "{name}.schema.json is stale: run `UPDATE_SCHEMAS=1 cargo test --test reader_schemas`"
        );
        jsonschema::validator_for(&on_disk)
            .unwrap_or_else(|e| panic!("{name} schema is not a valid JSON Schema: {e}"));
    }
}

#[test]
fn input_schema_is_an_object_and_bounds_limits() {
    let s = generate::<ReadSessionInput>();
    assert_eq!(s["type"], "object", "MCP requires an object root");
    assert_eq!(
        s["additionalProperties"], false,
        "unknown fields are rejected"
    );
    assert_eq!(s["properties"]["max_bytes"]["minimum"], 4096);
    assert_eq!(s["properties"]["max_bytes"]["maximum"], 262144);
}

fn validator(name: &str) -> jsonschema::Validator {
    let v: Value = serde_json::from_slice(
        &std::fs::read(
            root()
                .join("schemas/read_session")
                .join(format!("{name}.schema.json")),
        )
        .unwrap(),
    )
    .unwrap();
    jsonschema::validator_for(&v).unwrap()
}

fn check(v: &jsonschema::Validator, instance: &Value, what: &str) {
    let errs: Vec<String> = v
        .iter_errors(instance)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(errs.is_empty(), "{what}: {errs:?}");
}

#[test]
fn example_exchanges_validate_against_the_schemas() {
    let (input, output, error) = (validator("input"), validator("output"), validator("error"));
    let mut n = 0;
    let mut kinds = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(root().join("examples/read_session")).expect("examples exist") {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let ex: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        check(&input, &ex["request"], &format!("{name} request"));
        match (ex.get("response"), ex.get("error")) {
            (Some(r), None) => {
                check(&output, r, &format!("{name} response"));
                kinds.insert("success");
            }
            (None, Some(e)) => {
                check(&error, e, &format!("{name} error"));
                kinds.insert("error");
            }
            _ => panic!("{name}: exactly one of response/error"),
        }
        n += 1;
    }
    assert!(
        n >= 8,
        "expected a representative set of examples, found {n}"
    );
    assert_eq!(
        kinds.len(),
        2,
        "both success and error exchanges are covered"
    );
}

#[test]
fn invalid_instances_are_rejected() {
    let input = validator("input");
    assert!(
        !input.is_valid(&serde_json::json!({"session_id":"s","bogus":1})),
        "unknown field"
    );
    assert!(
        !input.is_valid(&serde_json::json!({"session_id":"s","max_bytes":10})),
        "limit below minimum"
    );
    assert!(!input.is_valid(&serde_json::json!({"session_id":"s","selection":{"kind":"match","match_ref":"x","before":99}})), "context above 20");
    assert!(input.is_valid(&serde_json::json!({"cursor":"c","max_items":5})));
    let error = validator("error");
    assert!(
        !error.is_valid(
            &serde_json::json!({"error":{"code":"NOPE","retryable":false,"message":"m"}})
        )
    );
}
