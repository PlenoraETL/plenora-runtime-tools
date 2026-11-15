use arrow_array::{
    RecordBatch,
    array::{BinaryArray, StringArray},
};
use arrow_schema::{DataType, Field, Metadata, Schema};
use plenora_io_model::{
    contract::FieldId,
    contract::{GeometryColumnContract, GeometryType},
    crs::ResolvedCrs,
};
use std::sync::Arc;
pub fn batch(
    primary: Option<&str>,
    count: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    let wkb = [
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
    ];
    for (i, id) in [17, 91].into_iter().take(count).enumerate() {
        let name = format!("g{i}");
        let mut geometry = GeometryColumnContract::wkb_xy(
            FieldId(u32::try_from(i)?),
            name.clone(),
            ResolvedCrs::wgs84(),
            true,
        );
        geometry.identita_dichiarata = Some(id);
        geometry.set_exact_geometry_types(vec![GeometryType::Point]);
        let mut md = Metadata::new();
        md.insert("ARROW:extension:name", "geoarrow.wkb");
        fields.push(plenora_io_model::geometry::with_geometry_contract_metadata(
            &Field::new(name, DataType::Binary, true).with_metadata(md),
            &geometry,
        ));
        arrays.push(Arc::new(BinaryArray::from(vec![
            Some(wkb.as_slice()),
            None,
            Some(wkb.as_slice()),
        ])));
    }
    let mut md = Metadata::new();
    md.insert("plenora.field_id", "103");
    fields.push(Field::new("value", DataType::Utf8, true).with_metadata(md));
    arrays.push(Arc::new(StringArray::from(vec![
        None,
        Some(""),
        Some("hello"),
    ])));
    let mut md = Metadata::new();
    md.insert("plenora.contract.version", "1");
    if let Some(primary) = primary {
        md.insert("plenora.primary_geometry", primary);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, md)),
        arrays,
    )?)
}
pub fn ipc(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}
