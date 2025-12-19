use minicbor_serde::{from_slice, to_vec};
use serde_json::Value;

pub fn decode_to_vec(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let value: Value = from_slice(bytes).map_err(|e| format!("cbor::decode_to_vec: {:?}", e))?;

    serde_json::to_vec(&value)
        .map_err(|e| format!("cbor::decode_to_vec: serde_json::to_vec: {}", e))
}

pub fn decode(bytes: &[u8]) -> Result<Value, String> {
    from_slice(bytes).map_err(|e| format!("cbor::decode: {:?}", e))
}

pub fn encode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let json: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("Invalid JSON: {}", e))?;
    to_vec(json).map_err(|e| format!("cbor::encode: {:?}", e))
}
