//! The request and response envelopes of the protocol.

use serde::Deserialize;
use serde_json::{Value, json};

use crate::records::PROTOCOL_VERSION;

/// A parsed request envelope.
#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    pub kind: String,
    #[serde(default)]
    pub head: RequestHead,
    #[serde(default)]
    pub data: Value,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RequestHead {
    #[serde(rename = "corrId", default)]
    pub corr_id: String,
    #[serde(default)]
    pub version: Option<String>,
}

/// The outcome of one request: a status and the `data` of the response.
/// For an error, `data` is the message text.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Reply {
    Ok(u16, Value),
    Err(u16, String),
}

impl Reply {
    pub fn ok(data: Value) -> Self {
        Reply::Ok(200, data)
    }

    pub fn err(status: u16, message: impl Into<String>) -> Self {
        Reply::Err(status, message.into())
    }

    pub fn status(&self) -> u16 {
        match self {
            Reply::Ok(status, _) | Reply::Err(status, _) => *status,
        }
    }

    /// The reply as the `data` field of an envelope.
    pub fn into_data(self) -> Value {
        match self {
            Reply::Ok(_, data) => data,
            Reply::Err(_, message) => Value::String(message),
        }
    }
}

/// Parse a request envelope. A malformed envelope gets a 400 reply
/// with the kind and the correlation id of the envelope, where present.
pub(crate) fn parse_request(raw: &str) -> Result<Request, (String, String, Reply)> {
    let value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(e) => {
            return Err((
                String::new(),
                String::new(),
                Reply::err(400, format!("Invalid request: {e}")),
            ));
        }
    };
    let kind = value["kind"].as_str().unwrap_or_default().to_string();
    let corr_id = value["head"]["corrId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let request: Request = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(e) => {
            return Err((
                kind,
                corr_id,
                Reply::err(400, format!("Invalid request: {e}")),
            ));
        }
    };
    if request.kind.is_empty() {
        return Err((kind, corr_id, Reply::err(400, "Request kind is required")));
    }
    if let Some(version) = &request.head.version
        && version != PROTOCOL_VERSION
    {
        return Err((
            kind,
            corr_id,
            Reply::err(400, format!("Unsupported protocol version: {version}")),
        ));
    }
    if !request.data.is_object() {
        return Err((
            kind,
            corr_id,
            Reply::err(400, "Request data must be an object"),
        ));
    }
    Ok(request)
}

/// Build a response envelope.
pub(crate) fn response(kind: &str, corr_id: &str, reply: Reply) -> Value {
    json!({
        "kind": kind,
        "head": {
            "corrId": corr_id,
            "status": reply.status(),
            "version": PROTOCOL_VERSION,
        },
        "data": reply.into_data(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_malformed_envelope_gets_a_400_with_its_kind_and_corr_id() {
        let raw =
            r#"{"kind":"promise.get","head":{"corrId":"c1","version":"2026-04-01"},"data":[]}"#;
        let (kind, corr_id, reply) = parse_request(raw).unwrap_err();
        assert_eq!(kind, "promise.get");
        assert_eq!(corr_id, "c1");
        assert_eq!(reply.status(), 400);
        let envelope = response(&kind, &corr_id, reply);
        assert_eq!(envelope["head"]["status"], 400);
        assert_eq!(envelope["head"]["version"], PROTOCOL_VERSION);
        assert!(envelope["data"].is_string());
    }

    #[test]
    fn an_unsupported_version_is_refused() {
        let raw =
            r#"{"kind":"promise.get","head":{"corrId":"c1","version":"2025-01-01"},"data":{}}"#;
        let (_, _, reply) = parse_request(raw).unwrap_err();
        assert_eq!(reply.status(), 400);
    }
}
