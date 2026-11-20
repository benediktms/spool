use spool::config::Config;
use spool::domain::HarnessKind;
use spool::source::SourceError;
use spool::sources::{SourceRegistry, set_enabled_in_file};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

fn opencode_source(roots: &str, opt_in: bool) -> String {
    let enabled = if opt_in { "enabled = true" } else { "" };
    format!(
        r#"
[[sources]]
id = "fixture"
kind = "opencode"
roots = {roots}
{enabled}
"#
    )
}

#[test]
fn opencode_config_requires_one_explicit_absolute_database_path() {
    let base = Path::new("/synthetic/spool");
    for roots in [
        "[]",
        r#"["relative.db"]"#,
        r#"["~/opencode.db"]"#,
        r#"["/"]"#,
        r#"["/synthetic/one.db", "/synthetic/two.db"]"#,
    ] {
        let err = Config::parse(&opencode_source(roots, true), base).unwrap_err();
        assert!(
            err.to_string()
                .contains("requires exactly one absolute database path"),
            "{err}"
        );
    }

    let path = "/synthetic/open code.db";
    let text = opencode_source(&format!("[{path:?}]"), true);
    let cfg = Config::parse(&text, base).unwrap();
    assert_eq!(cfg.sources[0].kind, HarnessKind::OpenCode);
    assert_eq!(cfg.sources[0].kind.as_str(), "opencode");
    assert_eq!(cfg.sources[0].roots, [path]);
    assert_eq!(Config::parse(&cfg.show(), base).unwrap(), cfg);
    assert!(
        Config::default()
            .sources
            .iter()
            .all(|s| s.kind != HarnessKind::OpenCode || !s.enabled)
    );
}

#[test]
fn opencode_registry_opts_in_without_using_the_jsonl_route() {
    let base = Path::new("/synthetic/spool");
    let path = "/synthetic/open code.db";
    let roots = &format!("[{path:?}]");
    let mut cfg = Config::parse(&opencode_source(roots, false), base).unwrap();
    let registry = SourceRegistry::new(&cfg).unwrap();
    assert_eq!(registry.opencode_database_path("fixture"), None);

    cfg.sources[0].enabled = true;
    registry.reload(&cfg).unwrap();
    assert_eq!(
        registry.opencode_database_path("fixture"),
        Some(PathBuf::from(path))
    );
    assert_eq!(registry.opencode_database_path("unknown"), None);
    assert!(registry.is_enabled("fixture"));
    assert!(registry.entry("fixture").unwrap().enabled);
    assert!(registry.entries()[0].enabled);
    assert_eq!(registry.enabled_ids(), ["fixture"]);
    assert!(registry.blob_roots("fixture").is_empty());
    assert!(
        registry
            .scan_from("fixture", &AtomicBool::new(false), 1, None)
            .is_none()
    );
    assert!(matches!(
        registry.open("fixture", Path::new(path)),
        Err(SourceError::OutsideRoot)
    ));

    cfg.sources[0].kind = HarnessKind::Codex;
    registry.reload(&cfg).unwrap();
    assert_eq!(registry.opencode_database_path("fixture"), None);
    assert!(registry.is_enabled("fixture"));
}

#[test]
fn opencode_can_be_enabled_and_disabled() {
    let temp = tempfile::tempdir().unwrap();
    let config_file = temp.path().join("config.toml");
    let text = opencode_source(r#"["/synthetic/opencode.db"]"#, false);
    std::fs::write(&config_file, &text).unwrap();

    for enabled in [true, false] {
        set_enabled_in_file(&config_file, "fixture", enabled).unwrap();
        let config =
            Config::parse(&std::fs::read_to_string(&config_file).unwrap(), temp.path()).unwrap();
        assert_eq!(config.sources[0].enabled, enabled);
    }
}
