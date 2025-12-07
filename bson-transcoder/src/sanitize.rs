use serde_json::{Map, Number, Value};

const MIN_TIME_MILLIS: i64 = -62167219200000; // year 0
const MAX_TIME_MILLIS: i64 = 253402300799999; // year 9999

/// Sanitize a JSON document, converting Extended JSON types to simplified format.
/// Also handles _id stringification at the top level.
pub fn sanitize_document(doc: Map<String, Value>) -> Map<String, Value> {
    doc.into_iter()
        .map(|(key, value)| {
            if key == "_id" {
                (key, Value::String(id_to_string(value)))
            } else {
                (key, sanitize_value(value))
            }
        })
        .collect()
}

/// Convert _id value to string.
/// _id is always either a string or ObjectId.
pub fn id_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Object(obj) => {
            // ObjectId: {"$oid": "hexstring"}
            if let Some(Value::String(hex)) = obj.get("$oid") {
                return hex.clone();
            }
            // Fallback: JSON-serialize the object
            serde_json::to_string(&Value::Object(obj)).unwrap_or_default()
        }
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

/// Recursively sanitize a JSON value.
fn sanitize_value(value: Value) -> Value {
    match value {
        Value::Object(obj) => sanitize_object(obj),
        Value::Array(arr) => Value::Array(arr.into_iter().map(sanitize_value).collect()),
        Value::Number(n) => sanitize_number(n),
        other => other,
    }
}

/// Sanitize a JSON object - check if it's an Extended JSON type.
fn sanitize_object(obj: Map<String, Value>) -> Value {
    // Check for Extended JSON patterns with single key
    if obj.len() == 1 {
        if let Some(Value::String(hex)) = obj.get("$oid") {
            // ObjectId -> hex string
            return Value::String(hex.clone());
        }
        if let Some(Value::String(decimal)) = obj.get("$numberDecimal") {
            // Decimal128 -> string
            return Value::String(decimal.clone());
        }
    }

    // DateTime: {"$date": {"$numberLong": "millis"}} or {"$date": millis}
    if let Some(date_val) = obj.get("$date") {
        let millis = extract_datetime_millis(date_val);
        let clamped = millis.clamp(MIN_TIME_MILLIS, MAX_TIME_MILLIS);
        return Value::Number(Number::from(clamped));
    }

    // Binary: {"$binary": {"base64": "...", "subType": "..."}}
    if let Some(Value::Object(bin)) = obj.get("$binary") {
        if let Some(Value::String(b64)) = bin.get("base64") {
            return Value::String(b64.clone());
        }
    }

    // Not an Extended JSON type - recursively sanitize nested fields
    Value::Object(
        obj.into_iter()
            .map(|(k, v)| (k, sanitize_value(v)))
            .collect(),
    )
}

/// Extract milliseconds from DateTime Extended JSON.
fn extract_datetime_millis(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::Object(obj) => {
            // {"$numberLong": "millis"}
            if let Some(Value::String(s)) = obj.get("$numberLong") {
                s.parse().unwrap_or(0)
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Sanitize special float values (NaN, Infinity).
fn sanitize_number(n: Number) -> Value {
    if let Some(f) = n.as_f64() {
        if f.is_nan() {
            return Value::String("NaN".to_string());
        } else if f.is_infinite() {
            return Value::String(if f.is_sign_positive() {
                "Infinity"
            } else {
                "-Infinity"
            }
            .to_string());
        }
    }
    Value::Number(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sanitize_object_id() {
        let input = json!({"$oid": "507f1f77bcf86cd799439011"});
        let result = sanitize_value(input);
        assert_eq!(result, json!("507f1f77bcf86cd799439011"));
    }

    #[test]
    fn test_sanitize_datetime() {
        let input = json!({"$date": {"$numberLong": "1234567890000"}});
        let result = sanitize_value(input);
        assert_eq!(result, json!(1234567890000_i64));
    }

    #[test]
    fn test_sanitize_datetime_clamped_min() {
        // Year before 0
        let input = json!({"$date": {"$numberLong": "-100000000000000"}});
        let result = sanitize_value(input);
        assert_eq!(result, json!(MIN_TIME_MILLIS));
    }

    #[test]
    fn test_sanitize_datetime_clamped_max() {
        // Year after 9999
        let input = json!({"$date": {"$numberLong": "500000000000000"}});
        let result = sanitize_value(input);
        assert_eq!(result, json!(MAX_TIME_MILLIS));
    }

    #[test]
    fn test_sanitize_decimal128() {
        let input = json!({"$numberDecimal": "123.456"});
        let result = sanitize_value(input);
        assert_eq!(result, json!("123.456"));
    }

    #[test]
    fn test_sanitize_binary() {
        let input = json!({"$binary": {"base64": "SGVsbG8gV29ybGQ=", "subType": "00"}});
        let result = sanitize_value(input);
        assert_eq!(result, json!("SGVsbG8gV29ybGQ="));
    }

    #[test]
    fn test_sanitize_nested_document() {
        let input = json!({
            "name": "test",
            "created": {"$date": {"$numberLong": "1234567890000"}},
            "ref": {"$oid": "507f1f77bcf86cd799439011"}
        });
        let result = sanitize_value(input);
        assert_eq!(
            result,
            json!({
                "name": "test",
                "created": 1234567890000_i64,
                "ref": "507f1f77bcf86cd799439011"
            })
        );
    }

    #[test]
    fn test_sanitize_array_with_extended_json() {
        let input = json!([
            {"$oid": "507f1f77bcf86cd799439011"},
            {"$date": {"$numberLong": "1234567890000"}}
        ]);
        let result = sanitize_value(input);
        assert_eq!(result, json!(["507f1f77bcf86cd799439011", 1234567890000_i64]));
    }

    #[test]
    fn test_id_to_string_string() {
        let input = json!("my-string-id");
        assert_eq!(id_to_string(input), "my-string-id");
    }

    #[test]
    fn test_id_to_string_object_id() {
        let input = json!({"$oid": "507f1f77bcf86cd799439011"});
        assert_eq!(id_to_string(input), "507f1f77bcf86cd799439011");
    }

    #[test]
    fn test_sanitize_document_with_id() {
        let mut input = Map::new();
        input.insert(
            "_id".to_string(),
            json!({"$oid": "507f1f77bcf86cd799439011"}),
        );
        input.insert("name".to_string(), json!("test"));
        input.insert(
            "created".to_string(),
            json!({"$date": {"$numberLong": "1234567890000"}}),
        );

        let result = sanitize_document(input);
        assert_eq!(
            result.get("_id"),
            Some(&json!("507f1f77bcf86cd799439011"))
        );
        assert_eq!(result.get("name"), Some(&json!("test")));
        assert_eq!(result.get("created"), Some(&json!(1234567890000_i64)));
    }
}
