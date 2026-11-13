use std::{error::Error, str::FromStr};

use async_nats::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use plenora_runtime_messaging::{
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_TOTAL_BYTES, MessageMetadata,
};

use crate::{NatsAdapterError, NatsErrorCategory, NatsOperation};

const METADATA_PREFIX: &str = "Plenora-Meta-";
const CONTENT_TYPE: &str = "Plenora-Content-Type-B64";
const MAX_CONTENT_TYPE_BYTES: usize = MAX_METADATA_KEY_BYTES;
pub(crate) fn encode(
    content_type: &str,
    metadata: &MessageMetadata,
) -> Result<HeaderMap, NatsAdapterError> {
    if content_type.len() > MAX_CONTENT_TYPE_BYTES {
        return Err(protocol_limit(
            "content type exceeds the configured byte limit",
        ));
    }
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        CONTENT_TYPE,
        &URL_SAFE_NO_PAD.encode(content_type.as_bytes()),
    )?;
    for (key, value) in metadata.iter() {
        let name = format!(
            "{METADATA_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(key.as_bytes())
        );
        insert_header(&mut headers, &name, &URL_SAFE_NO_PAD.encode(value))?;
    }
    Ok(headers)
}

fn decode_raw(
    headers: Option<&HeaderMap>,
) -> Result<(String, Vec<(String, bytes::Bytes)>), NatsAdapterError> {
    let mut content_type = String::from("application/octet-stream");
    let mut metadata = Vec::<(String, bytes::Bytes)>::new();
    let Some(headers) = headers else {
        return Ok((content_type, metadata));
    };

    let mut metadata_entries = 0_usize;
    let mut metadata_decoded_bytes = 0_usize;
    let mut content_type_seen = false;
    for (name, values) in headers.iter() {
        let name = name.to_string();
        // RT-017: preserve ambiguity as a rejection, including empty value lists.
        if (name.eq_ignore_ascii_case(CONTENT_TYPE)
            || strip_prefix_ignore_ascii_case(&name, METADATA_PREFIX).is_some())
            && values.len() != 1
        {
            return Err(protocol_limit("ambiguous encoded metadata"));
        }
        let Some(value) = values.first() else {
            continue;
        };
        if name.eq_ignore_ascii_case(CONTENT_TYPE) {
            if content_type_seen {
                return Err(protocol_limit("duplicate encoded content type"));
            }
            content_type_seen = true;
            if value.as_str().len()
                > base64_no_pad_encoded_limit(
                    plenora_runtime_messaging::MAX_INGRESS_CONTENT_TYPE_BYTES,
                )
            {
                return Err(protocol_limit(
                    "encoded content type exceeds the configured byte limit",
                ));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(value.as_str())
                .map_err(|error| protocol("invalid encoded content type", error))?;
            content_type = String::from_utf8(bytes)
                .map_err(|error| protocol("content type is not UTF-8", error))?;
        } else if let Some(encoded_key) = strip_prefix_ignore_ascii_case(&name, METADATA_PREFIX) {
            metadata_entries = metadata_entries.saturating_add(1);
            if metadata_entries > MAX_METADATA_ENTRIES {
                return Err(protocol_limit(
                    "encoded metadata exceeds the configured entry limit",
                ));
            }
            if encoded_key.len() > base64_no_pad_encoded_limit(MAX_METADATA_TOTAL_BYTES * 6) {
                return Err(protocol_limit(
                    "encoded metadata key exceeds the configured byte limit",
                ));
            }
            if value.as_str().len() > base64_no_pad_encoded_limit(MAX_METADATA_TOTAL_BYTES * 6) {
                return Err(protocol_limit(
                    "encoded metadata value exceeds the configured byte limit",
                ));
            }
            let key = URL_SAFE_NO_PAD
                .decode(encoded_key)
                .map_err(|error| protocol("invalid encoded metadata key", error))?;
            let key = String::from_utf8(key)
                .map_err(|error| protocol("metadata key is not UTF-8", error))?;
            let value = URL_SAFE_NO_PAD
                .decode(value.as_str())
                .map_err(|error| protocol("invalid encoded metadata value", error))?;
            metadata_decoded_bytes = metadata_decoded_bytes
                .saturating_add(key.len())
                .saturating_add(value.len());
            if metadata_decoded_bytes > MAX_METADATA_TOTAL_BYTES * 6 {
                return Err(protocol_limit("raw metadata allocation limit exceeded"));
            }
            if metadata.iter().any(|(present, _)| present == &key) {
                return Err(protocol_limit("duplicate decoded metadata"));
            }
            metadata.push((key, value.into()));
        }
    }
    Ok((content_type, metadata))
}

#[cfg(test)]
fn decode(headers: Option<&HeaderMap>) -> Result<(String, MessageMetadata), NatsAdapterError> {
    let (content_type, raw) = decode_raw(headers)?;
    if content_type.len() > MAX_CONTENT_TYPE_BYTES {
        return Err(protocol_limit(
            "content type exceeds portable encoder limit",
        ));
    }
    let mut metadata = MessageMetadata::new();
    for (key, value) in raw {
        metadata
            .insert(key, value)
            .map_err(|e| protocol("invalid decoded metadata", e))?;
    }
    Ok((content_type, metadata))
}

const fn base64_no_pad_encoded_limit(decoded_limit: usize) -> usize {
    let complete_groups = decoded_limit / 3;
    let remainder = decoded_limit % 3;
    complete_groups
        .saturating_mul(4)
        .saturating_add(match remainder {
            0 => 0,
            1 => 2,
            _ => 3,
        })
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), NatsAdapterError> {
    let name = HeaderName::from_str(name)
        .map_err(|error| protocol("metadata header name cannot be represented", error))?;
    let value = HeaderValue::from_str(value)
        .map_err(|error| protocol("metadata header value cannot be represented", error))?;
    headers.insert(name, value);
    Ok(())
}

fn strip_prefix_ignore_ascii_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .and_then(|_| value.get(prefix.len()..))
}

fn protocol<E>(message: &'static str, source: E) -> NatsAdapterError
where
    E: Error + Send + Sync + 'static,
{
    NatsAdapterError::with_source(
        NatsErrorCategory::Protocol,
        NatsOperation::Metadata,
        message,
        source,
    )
}

fn protocol_limit(message: &'static str) -> NatsAdapterError {
    NatsAdapterError::new(
        NatsErrorCategory::Protocol,
        NatsOperation::Metadata,
        message,
    )
}

/// Decode the real NATS wire representation into an unadmitted message.
/// Lossy/unreadable metadata is carried explicitly to the canonical admission decoder.
/// This function does not invoke handlers or settle deliveries.
#[must_use]
pub fn decode_wire(
    headers: Option<&HeaderMap>,
    payload: bytes::Bytes,
    max_payload_bytes: usize,
) -> plenora_runtime_messaging::SerializedMessage {
    use plenora_runtime_messaging::SerializedMessage;
    let over_limit = payload.len() > max_payload_bytes;
    let message = match decode_raw(headers) {
        Ok((content_type, raw)) => {
            let mut metadata = MessageMetadata::new();
            let fits = raw
                .iter()
                .all(|(key, value)| metadata.insert(key.clone(), value.clone()).is_ok());
            let message = SerializedMessage::new(content_type, payload);
            if fits {
                message.with_headers(metadata)
            } else {
                message.with_raw_metadata(raw)
            }
        }
        Err(_) => {
            SerializedMessage::new("application/octet-stream", payload).with_unreadable_metadata()
        }
    };
    if over_limit {
        message.with_exceeded_transport_payload_limit()
    } else {
        message
    }
}
/// Encode the NATS metadata wire representation without performing admission.
/// # Errors
/// Rejects invalid sizes or an input already rejected by a receiving transport.
pub fn encode_wire(
    message: &plenora_runtime_messaging::SerializedMessage,
) -> Result<HeaderMap, NatsAdapterError> {
    if message.raw_metadata().is_some()
        || message.has_unreadable_metadata()
        || message.exceeds_transport_payload_limit()
    {
        return Err(protocol_limit(
            "rejected transport message cannot be forwarded",
        ));
    }
    encode(&message.content_type, &message.headers)
}

#[cfg(test)]
mod tests {
    use async_nats::HeaderMap;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use plenora_runtime_messaging::{MAX_METADATA_KEY_BYTES, MessageMetadata};

    use super::{
        CONTENT_TYPE, MAX_CONTENT_TYPE_BYTES, METADATA_PREFIX, decode, decode_wire, encode,
        insert_header,
    };

    #[test]
    fn duplicate_reserved_wire_values_are_rejected_before_map_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        for key in [
            CONTENT_TYPE.to_owned(),
            format!("{METADATA_PREFIX}cGxlbm9yYS5vcGVyYXRpb24udmVyc2lvbg"),
        ] {
            let mut headers = HeaderMap::new();
            let name: async_nats::HeaderName = key.parse()?;
            let value: async_nats::HeaderValue = "MQ".parse()?;
            headers.append(name.clone(), value.clone());
            headers.append(name, value);
            assert!(decode(Some(&headers)).is_err());
        }
        Ok(())
    }

    #[test]
    fn binary_metadata_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = MessageMetadata::new();
        input.insert("test.binary", vec![0, 255, 13, 10])?;
        let headers = encode("application/test+bin", &input)?;
        let (content_type, output) = decode(Some(&headers))?;
        assert_eq!(content_type, "application/test+bin");
        assert_eq!(input, output);
        Ok(())
    }

    #[test]
    fn oversized_content_type_is_rejected_before_encoding() -> Result<(), Box<dyn std::error::Error>>
    {
        let content_type = "a".repeat(MAX_CONTENT_TYPE_BYTES.saturating_add(1));
        let error = encode(&content_type, &MessageMetadata::new())
            .err()
            .ok_or("oversized content type must fail")?;
        assert_eq!(
            error.message(),
            "content type exceeds the configured byte limit"
        );
        Ok(())
    }

    #[test]
    fn decoded_content_type_above_encoder_bound_is_preserved_for_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let text = "a".repeat(MAX_CONTENT_TYPE_BYTES + 1);
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, CONTENT_TYPE, &URL_SAFE_NO_PAD.encode(&text))?;
        let message = decode_wire(Some(&headers), bytes::Bytes::new(), 1);
        assert_eq!(message.content_type.as_ref(), text);
        assert!(!message.has_unreadable_metadata());
        Ok(())
    }
    #[test]
    fn decoded_key_above_portable_bound_is_retained_raw() -> Result<(), Box<dyn std::error::Error>>
    {
        let key = "a".repeat(MAX_METADATA_KEY_BYTES + 1);
        let mut headers = HeaderMap::new();
        insert_header(
            &mut headers,
            &format!("{METADATA_PREFIX}{}", URL_SAFE_NO_PAD.encode(&key)),
            "eA",
        )?;
        let message = decode_wire(Some(&headers), bytes::Bytes::new(), 1);
        let raw = message.raw_metadata().ok_or("raw fields lost")?;
        assert_eq!(raw.first().ok_or("field")?.0, key);
        assert!(!message.has_unreadable_metadata());
        Ok(())
    }
    #[test]
    fn decoded_value_above_portable_bound_is_retained_raw() -> Result<(), Box<dyn std::error::Error>>
    {
        let value = vec![b'x'; plenora_runtime_messaging::MAX_METADATA_VALUE_BYTES + 1];
        let mut headers = HeaderMap::new();
        insert_header(
            &mut headers,
            &format!("{METADATA_PREFIX}dGVzdA"),
            &URL_SAFE_NO_PAD.encode(&value),
        )?;
        let message = decode_wire(Some(&headers), bytes::Bytes::new(), 1);
        let raw = message.raw_metadata().ok_or("raw fields lost")?;
        assert_eq!(raw.first().ok_or("field")?.1.as_ref(), value);
        assert!(!message.has_unreadable_metadata());
        Ok(())
    }
}
