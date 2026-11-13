//! Private conversion, called only after canonical lexical admission.
use plenora_runtime_capabilities::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityId, CapabilityMessageCodecError, CapabilityRequest,
    ContractId, INPUT_CONTRACT_METADATA_KEY, OPERATION_VERSION_METADATA_KEY, OperationName,
    OperationVersion,
};
use plenora_runtime_messaging::SerializedMessage;
pub(crate) fn decode(
    message: &SerializedMessage,
) -> Result<CapabilityRequest, CapabilityMessageCodecError> {
    let name = required_text(message, CAPABILITY_NAME_METADATA_KEY)?;
    let version = required_text(message, CAPABILITY_VERSION_METADATA_KEY)?
        .parse::<u16>()
        .map_err(|_error| {
            CapabilityMessageCodecError::InvalidVersion(CAPABILITY_VERSION_METADATA_KEY)
        })?;
    let operation = required_text(message, CAPABILITY_OPERATION_METADATA_KEY)?;
    let operation_version = required_text(message, OPERATION_VERSION_METADATA_KEY)?
        .parse::<u16>()
        .ok()
        .and_then(|value| OperationVersion::new(value).ok())
        .ok_or(CapabilityMessageCodecError::InvalidVersion(
            OPERATION_VERSION_METADATA_KEY,
        ))?;
    let input_contract = required_text(message, INPUT_CONTRACT_METADATA_KEY)?;
    let capability =
        CapabilityId::new(name, version).map_err(CapabilityMessageCodecError::Identifier)?;
    let operation =
        OperationName::new(operation).map_err(CapabilityMessageCodecError::Identifier)?;
    let input_contract =
        ContractId::new(input_contract).map_err(CapabilityMessageCodecError::Contract)?;
    let mut input = message.clone();
    let _name = input.headers.remove(CAPABILITY_NAME_METADATA_KEY);
    let _version = input.headers.remove(CAPABILITY_VERSION_METADATA_KEY);
    let _operation = input.headers.remove(CAPABILITY_OPERATION_METADATA_KEY);
    let _operation_version = input.headers.remove(OPERATION_VERSION_METADATA_KEY);
    let _input_contract = input.headers.remove(INPUT_CONTRACT_METADATA_KEY);
    Ok(CapabilityRequest::new(
        capability,
        operation,
        operation_version,
        input_contract,
        input,
    ))
}
fn required_text<'a>(
    message: &'a SerializedMessage,
    key: &'static str,
) -> Result<&'a str, CapabilityMessageCodecError> {
    message
        .headers
        .get_text(key)
        .map_err(|_error| CapabilityMessageCodecError::InvalidEncoding(key))?
        .ok_or(CapabilityMessageCodecError::Missing(key))
}
