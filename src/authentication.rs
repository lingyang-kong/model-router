//! Select a credential owner without validating credentials or trying an upstream.
use crate::{
    error::{Error, Result},
    protocol::header,
};
use axum::http::{HeaderMap, HeaderName};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Authentication {
    Subscription,
    ApiKey,
}

fn single_header(headers: &HeaderMap, name: HeaderName) -> Result<Option<&str>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Error::bad_request("Repeated authentication headers"));
    }
    value
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .map(Some)
        .ok_or_else(|| Error::bad_request("Invalid authentication header"))
}

pub(crate) fn classify(headers: &HeaderMap) -> Result<Authentication> {
    let authorization = single_header(headers, header::AUTHORIZATION)?
        .ok_or_else(|| Error::bad_request("Provider selection requires bearer authentication"))?;
    let token = authorization
        .split_once(' ')
        .filter(|(scheme, token)| scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty())
        .map(|(_, token)| token)
        .filter(|token| {
            token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(&byte))
        })
        .ok_or_else(|| Error::bad_request("Invalid bearer authentication"))?;
    let account = single_header(headers, header::CHATGPT_ACCOUNT_ID)?;
    if headers.contains_key(header::API_KEY) || headers.contains_key(header::X_API_KEY) {
        return Err(Error::bad_request("Conflicting authentication signals"));
    }
    let api_key = token.starts_with("sk-") && token.len() > 3;
    match (account, api_key) {
        (Some(account), false)
            if !account
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte == b',') =>
        {
            if [
                header::OPENAI_ORGANIZATION,
                header::OPENAI_PROJECT,
                header::X_OPENAI_ORGANIZATION,
                header::X_OPENAI_PROJECT,
            ]
            .iter()
            .any(|name| headers.contains_key(name))
            {
                return Err(Error::bad_request("Conflicting authentication signals"));
            }
            Ok(Authentication::Subscription)
        }
        (None, true) if !headers.contains_key(header::COOKIE) => Ok(Authentication::ApiKey),
        _ => Err(Error::bad_request(
            "Missing, conflicting, or unrecognized authentication signals",
        )),
    }
}
