//! File readback independent of the IO adapter and native DATA execution.
use arrow_array::{Array, BinaryArray, StringArray};
use std::path::Path;
#[allow(
    clippy::too_many_lines,
    reason = "Keep independent physical-file and semantic assertions in one oracle"
)]
pub fn output(root: &Path, format: &str, geometry: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (batch, schema) = if format == "ipc" {
        let mut reader = arrow_ipc::reader::StreamReader::try_new(
            std::fs::File::open(root.join("bundle/result.arrow"))?,
            None,
        )?;
        let batch = reader.next().ok_or("batch")??;
        assert!(reader.next().is_none());
        (batch, reader.schema())
    } else {
        let builder = s4_parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(root.join("bundle/result.parquet"))?,
        )?;
        let schema = builder.schema().clone();
        let mut reader = builder.build()?;
        let batch = reader.next().ok_or("batch")??;
        assert!(reader.next().is_none());
        (batch, schema)
    };
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch.num_columns(),
        if geometry {
            if format == "geoparquet" { 3 } else { 2 }
        } else {
            1
        }
    );
    if format == "geoparquet" {
        assert_eq!(schema.field(2).name(), "bbox");
        let bbox = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .ok_or("bbox")?;
        for (n, edge) in ["xmin", "ymin", "xmax", "ymax"].into_iter().enumerate() {
            let value = bbox
                .column_by_name(edge)
                .ok_or("edge")?
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or("float")?;
            assert!(value.is_null(0));
            assert_eq!(
                value.value(1).to_bits(),
                if n % 2 == 0 { 1.0_f64 } else { 2.0_f64 }.to_bits()
            );
            assert_eq!(value.value(2).to_bits(), value.value(1).to_bits());
        }
    }
    let text = batch
        .column(usize::from(geometry))
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("strings")?;
    assert_eq!(
        text.iter().collect::<Vec<_>>(),
        vec![Some(""), Some("hello"), None]
    );
    assert_eq!(
        schema
            .metadata()
            .get("plenora.contract.version")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        schema
            .field(usize::from(geometry))
            .metadata()
            .get("plenora.field_id")
            .map(String::as_str),
        Some("103")
    );
    if geometry {
        let wkb = [
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
        ];
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or("WKB")?;
        assert!(values.is_null(0));
        assert_eq!(values.value(1), wkb);
        assert_eq!(values.value(2), wkb);
        let field = schema.field(0).clone();
        assert_eq!(
            field.metadata().get("plenora.field_id").map(String::as_str),
            Some("17")
        );
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.wkb")
        );
        // Schema metadata is also checked when there is no first feature; source profile is fixed.
        let source = super::fixture::data::batch(None, 1)?;
        for (key, value) in source.schema().field(0).metadata() {
            assert_eq!(
                field.metadata().get(key),
                Some(value),
                "geometry metadata key {key}"
            );
        }
    }
    Ok(())
}
