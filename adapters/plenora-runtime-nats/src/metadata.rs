use std::{error::Error, str::FromStr};

use async_nats::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use plenora_runtime_messaging::{
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_TOTAL_BYTES,
    MAX_METADATA_VALUE_BYTES, MessageMetadata,
};

use crate::{NatsAdapterError, NatsErrorCategory, NatsOperation};

const METADATA_PREFIX: &str = "Plenora-Meta-";
const CONTENT_TYPE: &str = "Plenora-Content-Type-B64";
const MAX_CONTENT_TYPE_BYTES: usize = MAX_METADATA_KEY_BYTES;
const MAX_ENCODED_CONTENT_TYPE_BYTES: usize = base64_no_pad_encoded_limit(MAX_CONTENT_TYPE_BYTES);
const MAX_ENCODED_METADATA_KEY_BYTES: usize = base64_no_pad_encoded_limit(MAX_METADATA_KEY_BYTES);
const MAX_ENCODED_METADATA_VALUE_BYTES: usize =
    base64_no_pad_encoded_limit(MAX_METADATA_VALUE_BYTES);

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

pub(crate) fn decode(
    headers: Option<&HeaderMap>,
) -> Result<(String, MessageMetadata), NatsAdapterError> {
    let mut content_type = String::from("application/octet-stream");
    let mut metadata = MessageMetadata::new();
    let Some(headers) = headers else {
        return Ok((content_type, metadata));
    };

    let mut metadata_entries = 0_usize;
    let mut metadata_decoded_bytes = 0_usize;
    for (name, values) in headers.iter() {
        let name = name.to_string();
        let Some(value) = values.first() else {
            continue;
        };
        if name.eq_ignore_ascii_case(CONTENT_TYPE) {
            if value.as_str().len() > MAX_ENCODED_CONTENT_TYPE_BYTES {
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
            if encoded_key.len() > MAX_ENCODED_METADATA_KEY_BYTES {
                return Err(protocol_limit(
                    "encoded metadata key exceeds the configured byte limit",
                ));
            }
            if value.as_str().len() > MAX_ENCODED_METADATA_VALUE_BYTES {
                return Err(protocol_limit(
                    "encoded metadata value exceeds the configured byte limit",
                ));
            }
            let entry_decoded_bytes = decoded_len_upper_bound(encoded_key.len())
                .saturating_add(decoded_len_upper_bound(value.as_str().len()));
            metadata_decoded_bytes = metadata_decoded_bytes.saturating_add(entry_decoded_bytes);
            if metadata_decoded_bytes > MAX_METADATA_TOTAL_BYTES {
                return Err(protocol_limit(
                    "encoded metadata exceeds the configured total byte limit",
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
            metadata
                .insert(key, value)
                .map_err(|error| protocol("invalid decoded metadata key", error))?;
        }
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

const fn decoded_len_upper_bound(encoded_len: usize) -> usize {
    encoded_len.saturating_mul(3) / 4
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

#[cfg(test)]
mod tests {
    use async_nats::HeaderMap;
    use plenora_runtime_messaging::MessageMetadata;

    use super::{
        CONTENT_TYPE, MAX_CONTENT_TYPE_BYTES, MAX_ENCODED_CONTENT_TYPE_BYTES,
        MAX_ENCODED_METADATA_KEY_BYTES, MAX_ENCODED_METADATA_VALUE_BYTES, METADATA_PREFIX, decode,
        encode, insert_header,
    };

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
    fn oversized_encoded_content_type_is_rejected_before_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        insert_header(
            &mut headers,
            CONTENT_TYPE,
            &"A".repeat(MAX_ENCODED_CONTENT_TYPE_BYTES.saturating_add(1)),
        )?;
        let error = decode(Some(&headers)).err().ok_or("decode must fail")?;
        assert_eq!(
            error.message(),
            "encoded content type exceeds the configured byte limit"
        );
        Ok(())
    }

    #[test]
    fn oversized_encoded_metadata_key_is_rejected_before_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        let name = format!(
            "{METADATA_PREFIX}{}",
            "A".repeat(MAX_ENCODED_METADATA_KEY_BYTES.saturating_add(1))
        );
        insert_header(&mut headers, &name, "AA")?;
        let error = decode(Some(&headers)).err().ok_or("decode must fail")?;
        assert_eq!(
            error.message(),
            "encoded metadata key exceeds the configured byte limit"
        );
        Ok(())
    }

    #[test]
    fn oversized_encoded_metadata_value_is_rejected_before_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        insert_header(
            &mut headers,
            &format!("{METADATA_PREFIX}dGVzdA"),
            &"A".repeat(MAX_ENCODED_METADATA_VALUE_BYTES.saturating_add(1)),
        )?;
        let error = decode(Some(&headers)).err().ok_or("decode must fail")?;
        assert_eq!(
            error.message(),
            "encoded metadata value exceeds the configured byte limit"
        );
        Ok(())
    }
}
