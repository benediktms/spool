use super::*;
use crate::adapters::opencode::ProjectedMessage;
use crate::clock::ManualClock;
use crate::config::SourceConfig;
use crate::domain::HarnessKind;
use crate::source::opencode::{self, ScanControl};
use serde_json::json;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Executor, SqliteConnection};
use std::sync::atomic::AtomicBool;

struct NoStager;

impl SourceReadStager for NoStager {
    fn stage_source_read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<StagedRead, AppError>> {
        panic!("fixture must read pinned records without staging")
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
    writer: SqliteConnection,
    read: ReadService,
    session: String,
    sizes: Vec<u64>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native.db");
        let mut writer = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        writer
            .execute(
                "CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT,title TEXT,time_updated INTEGER);
                 CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT);
                 CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT);
                 CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER);
                 INSERT INTO session_v2 VALUES ('native',NULL,'/synthetic','fixture',1),('other',NULL,'/synthetic',NULL,1);
                 INSERT INTO session_message VALUES
                   ('first','native','user',1,'{\"text\":\"first ✓\"}'),
                   ('second','native','assistant',2,'{\"content\":[]}'),
                   ('second','other','user',1,'{\"text\":\"different session\"}');
                 INSERT INTO session_part VALUES
                   ('second','native',0,'reasoning','{\"text\":\"this excluded reasoning still costs source bytes\"}'),
                    ('second','native',2,'text','{\"text\":\"second answer\"}');
                  INSERT INTO event_sequence VALUES ('native',2),('other',1);",
            )
            .await
            .unwrap();
        let index = Index::open_for_test(dir.path()).await.unwrap();
        sqlx::query(
            "INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('native','opencode',?,1)",
        )
        .bind(json!([&path]).to_string())
        .execute(&index.writer)
        .await
        .unwrap();
        let control = ScanControl {
            cancel: &AtomicBool::new(false),
            deadline: None,
        };
        let mut cfg = Config {
            sources: vec![SourceConfig {
                id: "native".into(),
                kind: HarnessKind::OpenCode,
                enabled: true,
                roots: vec![path.to_str().unwrap().to_owned()],
                include_subagents: false,
            }],
            ..Config::default()
        };
        cfg.mcp.allow_raw_records = true;
        cfg.discovery.debounce_seconds = 0;
        let cfg = Arc::new(cfg);
        let sources = Arc::new(SourceRegistry::new(&cfg).unwrap());
        let report = crate::discovery::DiscoveryService::new(
            index.clone(),
            sources.clone(),
            cfg.clone(),
            Arc::new(ManualClock::new(1)),
        )
        .run_auto("native", Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
        assert!(report.complete, "{:?}", report.error);
        let session: String = sqlx::query_scalar(
            "SELECT id FROM sessions WHERE source_id='native' AND native_id='native'",
        )
        .fetch_one(&index.reader)
        .await
        .unwrap();
        let mut guarded = opencode::open(&path).await.unwrap();
        let messages = opencode::project_session(&mut guarded, "native", 4096, &control)
            .await
            .unwrap();
        let sizes = messages.iter().map(|m| m.source_bytes as u64).collect();
        seed_snapshot(&index, &session, messages).await;

        let leases = LeaseManager::new(
            index.clone(),
            cfg.reader.clone(),
            Arc::new(ManualClock::new(1)),
        );
        let key_path = dir.path().join("test.key");
        HandleKey::create(&key_path).unwrap();
        let keys = KeyStore::load(&key_path).unwrap();
        let read = ReadService::new(index, cfg, leases, keys, sources, Arc::new(NoStager));
        Self {
            _dir: dir,
            path,
            writer,
            read,
            session,
            sizes,
        }
    }

    async fn first_page(&self) -> Value {
        self.read
            .call(json!({"session_id":self.session,"max_items":1,"verbose":true}))
            .await
            .unwrap()
    }
}

async fn seed_snapshot(index: &Index, session: &str, messages: Vec<ProjectedMessage>) {
    let mut tx = index.writer.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO snapshots(id,session_id,kind,state,source_revision,fingerprint,generation,prefix_end,created_at)
         VALUES ('fixture',?,'indexed','published','native-fixture','fixture',1,0,1)",
    )
    .bind(session)
    .execute(&mut *tx)
    .await
    .unwrap();
    let mut parent = None;
    for message in &messages {
        sqlx::query(
            "INSERT INTO records(snapshot_id,key,ord,native_id,parent_key,kind,offset,len,digest,locator_payload)
             VALUES ('fixture',?1,?2,?1,?3,'message',0,0,?4,?5)",
        )
        .bind(&message.id)
        .bind(message.seq)
        .bind(parent)
        .bind(message.digest.as_slice())
        .bind(json!({"kind":"opencode_sqlite_v1","session_id":"native","message_id":message.id}).to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO messages(snapshot_id,record_key,seq,role,parent_message_key)
             VALUES ('fixture',?,?,?,?)",
        )
        .bind(&message.id)
        .bind(message.seq)
        .bind(&message.role)
        .bind(parent)
        .execute(&mut *tx)
        .await
        .unwrap();
        for block in &message.blocks {
            sqlx::query("INSERT INTO blocks(snapshot_id,record_key,idx,origin,text,text_digest) VALUES ('fixture',?,?,'authored',?,?)")
                .bind(&message.id)
                .bind(block.index)
                .bind(&block.text)
                .bind(crate::domain::digest(block.text.as_bytes()).as_slice())
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        parent = Some(&message.id);
    }
    sqlx::query("UPDATE sessions SET active_snapshot_id='fixture' WHERE id=?")
        .bind(session)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn opencode_reader_pins_cursor_and_detects_rewrite_and_deletion() {
    let mut f = Fixture::new().await;
    let first = f.first_page().await;
    assert_eq!(first["items"][0]["blocks"][0]["data"], "first ✓");
    assert_eq!(first["items"][0]["provenance"]["origin"], "verified_source");
    assert_eq!(first["items"][0]["provenance"]["native_record_id"], "first");
    assert!(first["items"][0]["provenance"]["locator"].is_null());
    let cursor = json!({"cursor":first["page"]["next_cursor"]});

    f.writer.execute(
        "INSERT INTO session_message VALUES ('append','native','user',3,'{\"text\":\"not pinned\"}');
         UPDATE session_message SET data='{\"text\":\"unrelated write\"}' WHERE session_id='other';"
    ).await.unwrap();
    let next = f.read.call(cursor.clone()).await.unwrap();
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_eq!(next["items"][0]["blocks"][0]["data"], "second answer");
    assert_eq!(next["items"][0]["provenance"]["origin"], "verified_source");
    assert_eq!(next["page"]["selection_exhausted"], true);

    f.writer
        .execute("UPDATE session_part SET data='{\"text\":\"changed\"}' WHERE ordinal=2")
        .await
        .unwrap();
    assert_eq!(
        f.read.call(cursor.clone()).await.unwrap_err().code,
        "SOURCE_CHANGED"
    );
    f.writer
        .execute(
            "UPDATE session_part SET data='{\"text\":\"second answer\"}' WHERE ordinal=2;
        DELETE FROM session_v2 WHERE id='native';",
        )
        .await
        .unwrap();
    assert_eq!(
        f.read.call(cursor).await.unwrap_err().code,
        "SOURCE_CHANGED"
    );
}

#[tokio::test]
async fn opencode_reader_distinguishes_absent_and_replaced_database() {
    let f = Fixture::new().await;
    f.writer.close().await.unwrap();
    let request = json!({"session_id":f.session});
    let moved = f.path.with_extension("old");
    std::fs::rename(&f.path, &moved).unwrap();

    let result = f.read.call(request.clone()).await.unwrap();
    assert_eq!(
        result["items"][0]["provenance"]["origin"],
        "indexed_snapshot"
    );
    assert_eq!(result["warnings"][0]["code"], "SOURCE_FALLBACK");
    assert_eq!(
        f.read
            .call(json!({"session_id":f.session,"source_policy":"require_source"}))
            .await
            .unwrap_err()
            .code,
        "SOURCE_UNAVAILABLE"
    );
    assert_eq!(
        f.read
            .call(json!({"session_id":f.session,"view":"raw_records"}))
            .await
            .unwrap_err()
            .code,
        "VIEW_NOT_SUPPORTED"
    );
    std::fs::copy(&moved, &f.path).unwrap();
    assert_eq!(
        f.read.call(request).await.unwrap_err().code,
        "SOURCE_CHANGED"
    );
    let indexed = f
        .read
        .call(json!({"session_id":f.session,"source_policy":"indexed_only"}))
        .await
        .unwrap();
    assert_eq!(
        indexed["items"][1]["provenance"]["origin"],
        "indexed_snapshot"
    );
}

#[tokio::test]
async fn opencode_reader_checks_pinned_session_and_both_byte_budgets() {
    let mut f = Fixture::new().await;
    let request = json!({"session_id":f.session});
    Arc::make_mut(&mut f.read.cfg).reader.max_source_read_bytes = f.sizes.iter().sum::<u64>() - 1;
    let error = f.read.call(request.clone()).await.unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
    assert_eq!(error.details.unwrap()["limit"], "max_source_read_bytes");

    let cfg = Arc::make_mut(&mut f.read.cfg);
    cfg.reader.max_source_read_bytes = 4096;
    cfg.ingestion.max_record_bytes = f.sizes[1] - 1;
    let error = f.read.call(request.clone()).await.unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
    assert_eq!(error.details.unwrap()["limit"], "max_record_bytes");

    sqlx::query("UPDATE records SET locator_payload=? WHERE key='second'")
        .bind(
            json!({"kind":"opencode_sqlite_v1","session_id":"other","message_id":"second"})
                .to_string(),
        )
        .execute(&f.read.index.writer)
        .await
        .unwrap();
    assert_eq!(
        f.read.call(request).await.unwrap_err().code,
        "SOURCE_CHANGED"
    );
}
