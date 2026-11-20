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
    for (case, expectation) in oracle.as_object().unwrap() {
        let find = |capture: &Value, target: i32| {
            capture["calls"]
                .as_array()
                .unwrap()
                .iter()
                .find(|call| {
                    call["case"] == *case && call["target"] == target && call.get("kind").is_none()
                })
                .unwrap()
                .clone()
        };
        let count = |capture: &Value, target: i32| {
            capture["calls"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|call| call["case"] == *case && call["target"] == target)
                .count()
        };
        let old_calls = count(&baseline, 1200);
        let new_calls = count(&candidate, 512);
        println!("{case}: baseline calls={old_calls}, candidate calls={new_calls}");
        let old = find(&baseline, 1200);
        let new = find(&candidate, 512);
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
    for target in [1200, 512] {
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
