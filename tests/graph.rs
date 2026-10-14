use spool::{
    domain::{BlockOrigin, MessageEntry, RecordKind, RecordMeta, Role, SourceLocator, TextBlock},
    graph::SessionGraph,
};

fn record(key: &str, parent: Option<&str>) -> RecordMeta {
    RecordMeta {
        key: key.into(),
        native_id: Some(key.into()),
        parent_key: parent.map(str::to_owned),
        kind: RecordKind::Message,
        timestamp: None,
        turn_id: None,
        locator: SourceLocator { offset: 0, len: 1 },
        digest: [0; 32],
    }
}
fn message(key: &str, supersedes: &[&str]) -> MessageEntry {
    MessageEntry {
        record_key: key.into(),
        role: Role::Assistant,
        phase: None,
        blocks: vec![TextBlock {
            index: 0,
            origin: BlockOrigin::Authored,
            text: key.into(),
        }],
        attachments: vec![],
        supersedes: supersedes.iter().map(|key| (*key).into()).collect(),
    }
}

#[test]
fn child_parenting_resolves_to_authoritative_saved_message() {
    let records = [
        record("user", None),
        record("assistant-old", Some("user")),
        record("assistant-final", Some("user")),
        record("child", Some("assistant-old")),
    ];
    let messages = [
        MessageEntry {
            record_key: "user".into(),
            role: Role::User,
            phase: None,
            blocks: vec![TextBlock {
                index: 0,
                origin: BlockOrigin::Authored,
                text: "user".into(),
            }],
            attachments: vec![],
            supersedes: vec![],
        },
        message("assistant-final", &["assistant-old"]),
        message("child", &[]),
    ];
    let (graph, warnings) = SessionGraph::build(&records, &messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.message_parent("assistant-old"), Some("user"));
    assert_eq!(graph.message_parent("child"), Some("assistant-final"));
    assert_eq!(graph.path_to("child"), ["user", "assistant-final", "child"]);
    assert!(graph.is_ancestor("assistant-old", "child"));
}
