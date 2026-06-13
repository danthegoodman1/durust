use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Sample {
    value: String,
}

#[test]
fn messagepack_payload_round_trips() {
    let sample = Sample {
        value: "messagepack".to_owned(),
    };
    let payload = durust::encode_payload(&sample).unwrap();
    assert_eq!(durust::decode_payload::<Sample>(&payload).unwrap(), sample);
}

#[test]
fn json_payload_round_trips_when_explicitly_encoded() {
    let sample = Sample {
        value: "json".to_owned(),
    };
    let payload = durust::encode_payload_with_codec(&sample, durust::CodecId::Json).unwrap();
    assert_eq!(payload.decode_json::<Sample>().unwrap(), sample);
}

#[test]
fn codec_mismatch_fails_clearly() {
    let payload = durust::encode_payload_with_codec(&"json", durust::CodecId::Json).unwrap();
    let err = durust::decode_payload::<String>(&payload).unwrap_err();
    assert!(
        matches!(err, durust::Error::PayloadDecode(message) if message.contains("unsupported inline codec"))
    );
}

#[test]
fn blob_decode_requires_provider_hydration() {
    let payload = durust::encode_payload(&"large").unwrap();
    let blob = payload
        .to_blob_ref("memory://payload/test".to_owned())
        .unwrap();
    let err = durust::decode_payload::<String>(&blob).unwrap_err();
    assert!(
        matches!(err, durust::Error::PayloadDecode(message) if message.contains("hydrated by the durability provider"))
    );
}
