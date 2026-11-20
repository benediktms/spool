//! Optional analysis of two captures from fixtures/compact_lookup/capture.py.
use serde_json::Value;
use spool::mcp::budget::tool_result_bytes;

#[test]
fn analyze_compact_lookup_captures() {
    let Ok(root) = std::env::var("SPOOL_COMPACT_CAPTURE_DIR") else {
        return;
    };
    let load = |name: &str| -> Value {
        serde_json::from_slice(&std::fs::read(format!("{root}/{name}.json")).unwrap()).unwrap()
    };
    let baseline = load("baseline");
    let candidate = load("candidate");
    let oracle = load("oracle");
    assert_eq!(baseline["sources_sha256"], candidate["sources_sha256"]);
    for (name, capture, target) in [
        ("baseline-default", &baseline, Value::Null),
        ("candidate-default", &candidate, Value::Null),
    ] {
        let calls: Vec<_> = capture["calls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|call| call["target"] == target)
            .collect();
        let (mut total, mut search) = (0, 0);
        for call in &calls {
            let result = &call["response"]["result"];
            let value: Value =
                serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
            let bytes = tool_result_bytes(&value, result["isError"] == true);
            total += bytes;
            if call.get("kind").is_none() {
                search += bytes;
            }
        }
        println!(
            "{name}: calls={} total={total} search={search}",
            calls.len()
        );
    }
    for (case, expectation) in oracle.as_object().unwrap() {
        for (old_target, new_target) in [
            (Value::from(1200), Value::from(512)),
            (Value::Null, Value::Null),
        ] {
            let find = |capture: &Value, target: &Value| {
                capture["calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|call| {
                        call["case"] == *case
                            && &call["target"] == target
                            && call.get("kind").is_none()
                    })
                    .unwrap()
                    .clone()
            };
            let count = |capture: &Value, target: &Value| {
                capture["calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|call| call["case"] == *case && &call["target"] == target)
                    .count()
            };
            let old_calls = count(&baseline, &old_target);
            let new_calls = count(&candidate, &new_target);
            println!("{case}: baseline calls={old_calls}, candidate calls={new_calls}");
            let old = find(&baseline, &old_target);
            let new = find(&candidate, &new_target);
            assert_eq!(
                old["normalized"], new["normalized"],
                "search order or full-match anchors changed for {case}"
            );
            let result = |call: &Value| -> Value {
                serde_json::from_str(
                    call["response"]["result"]["content"][0]["text"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap()
            };
            let old = result(&old);
            let new = result(&new);
            let expected = expectation[1].as_str().unwrap();
            let previews = |value: &Value| {
                value["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|session| session["matches"].as_array().unwrap())
                    .filter_map(|m| m["preview"]["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            assert!(
                !previews(&old).contains(expected) || previews(&new).contains(expected),
                "compact preview lost a sufficient baseline preview for {case}"
            );
        }
    }
    for (case, _) in oracle.as_object().unwrap() {
        let calls = candidate["calls"].as_array().unwrap();
        let anchors = |target: Value| {
            &calls
                .iter()
                .find(|call| {
                    call["case"] == *case && call["target"] == target && call.get("kind").is_none()
                })
                .unwrap()["excerpts"]
        };
        assert!(
            !anchors(Value::Null).is_null(),
            "capture lacks excerpt anchors for {case}"
        );
        assert_eq!(
            anchors(Value::Null),
            anchors(Value::from(1200)),
            "default excerpt anchors changed for {case}"
        );
        assert_eq!(
            anchors(Value::from(512)),
            anchors(Value::from(1200)),
            "explicit excerpt anchors changed for {case}"
        );
    }
    for target in [Value::from(1200), Value::from(512), Value::Null] {
        for (name, capture) in [("baseline", &baseline), ("candidate", &candidate)] {
            let calls: Vec<_> = capture["calls"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|c| c["target"] == target)
                .collect();
            let mut total = 0;
            let mut search = 0;
            for call in &calls {
                let result = &call["response"]["result"];
                let value: Value =
                    serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
                let size = tool_result_bytes(&value, result["isError"] == true);
                total += size;
                if call.get("kind").is_none() {
                    search += size;
                }
            }
            for (case, expectation) in oracle.as_object().unwrap() {
                let expected = expectation[1].as_str().unwrap();
                let mut text = String::new();
                for call in calls
                    .iter()
                    .filter(|c| c["case"] == *case && c.get("kind").is_some())
                {
                    let result = &call["response"]["result"];
                    if result["isError"] == true {
                        continue;
                    }
                    let value: Value =
                        serde_json::from_str(result["content"][0]["text"].as_str().unwrap())
                            .unwrap();
                    for item in value["items"].as_array().into_iter().flatten() {
                        for block in item["blocks"].as_array().into_iter().flatten() {
                            text.push_str(block["data"].as_str().unwrap_or(""));
                        }
                    }
                }
                assert!(
                    text.contains(expected),
                    "{name} target={target} case={case} lacks its frozen oracle"
                );
            }
            println!(
                "{name} target={target} calls={} tool_result_bytes={total} search_bytes={search}",
                calls.len()
            );
        }
    }
}
