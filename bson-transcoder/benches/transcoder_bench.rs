use bson::raw::RawDocument;
use bson_transcoder::id_to_string;
use bson_transcoder::serializer::SanitizingSerializer;
use bson_transcoder::Utf8LossyDeserializer;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{Value, json};

// Helper functions to create BSON documents of varying sizes
fn create_native_bson_small() -> Vec<u8> {
    use bson::{doc, oid::ObjectId, DateTime, Decimal128};
    use std::str::FromStr;

    let doc = doc! {
        "_id": ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30i32,
        "created_at": DateTime::from_millis(1234567890000),
        "balance": Decimal128::from_str("1234.56").unwrap(),
        "active": true
    };
    doc.to_vec().unwrap()
}

fn create_native_bson_medium() -> Vec<u8> {
    use bson::{doc, oid::ObjectId, DateTime, Decimal128, Document};
    use std::str::FromStr;

    let mut doc = Document::new();
    doc.insert("_id", ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap());

    // Add 30 fields with various types
    for i in 0..10 {
        doc.insert(format!("string_field_{}", i), format!("value_{}", i));
        doc.insert(format!("number_field_{}", i), i * 100);
        doc.insert(format!("date_field_{}", i), DateTime::from_millis(1234567890000 + i as i64 * 1000));
    }

    // Add nested object
    let address = doc! {
        "street": "123 Main St",
        "city": "New York",
        "zip": "10001",
        "coordinates": {
            "lat": Decimal128::from_str("40.7128").unwrap(),
            "lng": Decimal128::from_str("-74.0060").unwrap()
        }
    };
    doc.insert("address", address);

    // Add array with ObjectIds
    doc.insert("tags", vec![
        ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
        ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap(),
        ObjectId::parse_str("507f1f77bcf86cd799439013").unwrap(),
    ]);

    doc.to_vec().unwrap()
}

fn create_native_bson_large() -> Vec<u8> {
    use bson::{doc, oid::ObjectId, DateTime, Decimal128, Document, Bson};
    use std::str::FromStr;

    let mut doc = Document::new();
    doc.insert("_id", ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap());

    // Add 100 fields
    for i in 0..50 {
        doc.insert(format!("field_{}", i), format!("value_{}", i));
        let nested = doc! {
            "a": ObjectId::parse_str(&format!("507f1f77bcf86cd79943901{:x}", i % 16)).unwrap(),
            "b": DateTime::from_millis(1234567890000 + i as i64 * 1000),
            "c": Decimal128::from_str(&format!("{}.{}", i, i * 2)).unwrap()
        };
        doc.insert(format!("nested_{}", i), nested);
    }

    // Add large array
    let mut large_array: Vec<Bson> = Vec::new();
    for i in 0..100 {
        let item = doc! {
            "id": ObjectId::parse_str(&format!("507f1f77bcf86cd79943{:04x}", i)).unwrap(),
            "value": i,
            "timestamp": DateTime::from_millis(1234567890000 + i as i64 * 1000)
        };
        large_array.push(Bson::Document(item));
    }
    doc.insert("items", large_array);

    // Add deeply nested structure
    let mut deep = doc! { "value": "leaf" };
    for _ in 0..10 {
        deep = doc! { "nested": deep, "date": DateTime::from_millis(1234567890000) };
    }
    doc.insert("deep_nested", deep);

    doc.to_vec().unwrap()
}

// Benchmarks

/// Transcode a raw BSON document to a serde_json::Value.
/// This produces Extended JSON format. This is used for benchmarking purposes (to compare against custom serializer).
pub fn bson_to_json(doc: &RawDocument) -> Result<Value, Box<dyn std::error::Error>> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let value: Value = serde_transcode::transcode(deserializer, serde_json::value::Serializer)?;
    Ok(value)
}

fn bench_id_to_string(c: &mut Criterion) {
    let oid = json!({"$oid": "507f1f77bcf86cd799439011"});
    let string_id = json!("my-string-id");

    let mut group = c.benchmark_group("id_to_string");

    group.bench_function("object_id", |b| {
        b.iter(|| id_to_string(black_box(oid.clone())))
    });

    group.bench_function("string_id", |b| {
        b.iter(|| id_to_string(black_box(string_id.clone())))
    });

    group.finish();
}

fn bench_bson_transcode(c: &mut Criterion) {
    let small_bson = create_native_bson_small();
    let medium_bson = create_native_bson_medium();
    let large_bson = create_native_bson_large();

    let mut group = c.benchmark_group("bson_transcode");

    for (name, bson_bytes) in [
        ("small", &small_bson),
        ("medium", &medium_bson),
        ("large", &large_bson),
    ] {
        group.throughput(Throughput::Bytes(bson_bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("to_value", name), bson_bytes, |b, bytes| {
            b.iter(|| {
                let raw = RawDocument::from_bytes(black_box(bytes)).unwrap();
                bson_to_json(black_box(&raw))
            })
        });
    }

    group.finish();
}

/// Transcode without Utf8LossyDeserializer (strict UTF-8)
fn bson_to_sanitized_json_strict<W: std::io::Write>(
    doc: &RawDocument,
    writer: W,
    is_root: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let mut serializer = SanitizingSerializer::new(writer, is_root);
    serde_transcode::transcode(deserializer, &mut serializer)?;
    Ok(())
}

/// Transcode with Utf8LossyDeserializer (lossy UTF-8)
fn bson_to_sanitized_json_lossy<W: std::io::Write>(
    doc: &RawDocument,
    writer: W,
    is_root: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let lossy_deserializer = Utf8LossyDeserializer::new(deserializer);
    let mut serializer = SanitizingSerializer::new(writer, is_root);
    serde_transcode::transcode(lossy_deserializer, &mut serializer)?;
    Ok(())
}

fn bench_bson_to_sanitized_json(c: &mut Criterion) {
    let small_bson = create_native_bson_small();
    let medium_bson = create_native_bson_medium();
    let large_bson = create_native_bson_large();

    let mut group = c.benchmark_group("bson_to_sanitized_json");

    for (name, bson_bytes) in [
        ("small", &small_bson),
        ("medium", &medium_bson),
        ("large", &large_bson),
    ] {
        group.throughput(Throughput::Bytes(bson_bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("transcode", name), bson_bytes, |b, bytes| {
            b.iter(|| {
                let raw = RawDocument::from_bytes(black_box(bytes)).unwrap();
                let mut output = Vec::new();
                bson_to_sanitized_json_strict(black_box(&raw), &mut output, true).unwrap();
                output
            })
        });
    }

    group.finish();
}

fn bench_utf8_lossy_overhead(c: &mut Criterion) {
    let small_bson = create_native_bson_small();
    let medium_bson = create_native_bson_medium();
    let large_bson = create_native_bson_large();

    let mut group = c.benchmark_group("utf8_lossy_overhead");

    for (name, bson_bytes) in [
        ("small", &small_bson),
        ("medium", &medium_bson),
        ("large", &large_bson),
    ] {
        group.throughput(Throughput::Bytes(bson_bytes.len() as u64));

        // Benchmark without Utf8LossyDeserializer (strict)
        group.bench_with_input(BenchmarkId::new("strict", name), bson_bytes, |b, bytes| {
            b.iter(|| {
                let raw = RawDocument::from_bytes(black_box(bytes)).unwrap();
                let mut output = Vec::new();
                bson_to_sanitized_json_strict(black_box(&raw), &mut output, true).unwrap();
                output
            })
        });

        // Benchmark with Utf8LossyDeserializer (lossy)
        group.bench_with_input(BenchmarkId::new("lossy", name), bson_bytes, |b, bytes| {
            b.iter(|| {
                let raw = RawDocument::from_bytes(black_box(bytes)).unwrap();
                let mut output = Vec::new();
                bson_to_sanitized_json_lossy(black_box(&raw), &mut output, true).unwrap();
                output
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_id_to_string,
    bench_bson_transcode,
    bench_bson_to_sanitized_json,
    bench_utf8_lossy_overhead,
);
criterion_main!(benches);
