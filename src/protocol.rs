//! Wire vocabulary shared by transport and discovery code.

pub(crate) mod header {
    pub use axum::http::header::{
        ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CACHE_CONTROL, CONNECTION, CONTENT_ENCODING,
        CONTENT_LENGTH, CONTENT_TYPE, COOKIE, ETAG, HOST, HeaderName, IF_MODIFIED_SINCE,
        IF_NONE_MATCH, LAST_MODIFIED, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
        SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION,
        TE, TRAILER, TRANSFER_ENCODING, UPGRADE, VARY,
    };

    pub const ANTHROPIC_VERSION: HeaderName = HeaderName::from_static("anthropic-version");
    pub const API_KEY: HeaderName = HeaderName::from_static("api-key");
    pub const CHATGPT_ACCOUNT_ID: HeaderName = HeaderName::from_static("chatgpt-account-id");
    pub const CONTENT_DIGEST: HeaderName = HeaderName::from_static("content-digest");
    pub const CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
    pub const DIGEST: HeaderName = HeaderName::from_static("digest");
    pub const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
    pub const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
    pub const OPENAI_ORGANIZATION: HeaderName = HeaderName::from_static("openai-organization");
    pub const OPENAI_PROJECT: HeaderName = HeaderName::from_static("openai-project");
    pub const ORIGINATOR: HeaderName = HeaderName::from_static("originator");
    pub const PROXY_CONNECTION: HeaderName = HeaderName::from_static("proxy-connection");
    pub const REPR_DIGEST: HeaderName = HeaderName::from_static("repr-digest");
    pub const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
    pub const X_MODEL: HeaderName = HeaderName::from_static("x-model");
    pub const X_OPENAI_ORGANIZATION: HeaderName = HeaderName::from_static("x-openai-organization");
    pub const X_OPENAI_PROJECT: HeaderName = HeaderName::from_static("x-openai-project");

    /// Credential and tenant headers shared by authentication and cache scoping.
    pub const CREDENTIALS: [HeaderName; 6] = [
        AUTHORIZATION,
        COOKIE,
        OPENAI_ORGANIZATION,
        OPENAI_PROJECT,
        X_API_KEY,
        API_KEY,
    ];

    pub const ACCOUNT_SCOPE: [HeaderName; 3] =
        [CHATGPT_ACCOUNT_ID, X_OPENAI_ORGANIZATION, X_OPENAI_PROJECT];

    pub const HOP_BY_HOP: [HeaderName; 9] = [
        HOST,
        CONNECTION,
        KEEP_ALIVE,
        TRANSFER_ENCODING,
        UPGRADE,
        TE,
        PROXY_AUTHORIZATION,
        PROXY_AUTHENTICATE,
        PROXY_CONNECTION,
    ];

    /// Integrity metadata becomes stale whenever body bytes change.
    pub const BODY_INTEGRITY: [HeaderName; 5] =
        [CONTENT_MD5, DIGEST, CONTENT_DIGEST, REPR_DIGEST, TRAILER];

    /// Handshake-only fields must not accompany ordinary HTTP requests.
    pub const WEBSOCKET_HANDSHAKE: [HeaderName; 4] = [
        SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_VERSION,
        SEC_WEBSOCKET_PROTOCOL,
        SEC_WEBSOCKET_EXTENSIONS,
    ];
}

pub(crate) mod media_type {
    pub const JSON: &str = "application/json";
    pub const EVENT_STREAM: &str = "text/event-stream";
}

pub(crate) mod encoding {
    pub const GZIP: &str = "gzip";
    pub const X_GZIP: &str = "x-gzip";
    pub const IDENTITY: &str = "identity";
    pub const DEFLATE: &str = "deflate";
    pub const BROTLI: &str = "br";
    pub const ZSTD: &str = "zstd";
}

pub(crate) mod endpoint {
    pub const RESPONSES: &str = "/responses";
    pub const TOOLS: &str = "/tools";
    pub const TOOLS_LIST: &str = "/tools/list";
}

pub(crate) mod token {
    pub const UPGRADE: &str = "upgrade";
    pub const WEBSOCKET: &str = "websocket";
    pub const TRAILERS: &str = "trailers";
}

pub(crate) mod pointer {
    pub const ERROR_CODE: &str = "/error/code";
    pub const ERROR_MESSAGE: &str = "/error/message";
    pub const INSTRUCTIONS_TEMPLATE: &str = "/model_messages/instructions_template";
}

/// Scalar values shared by Responses payloads and capability schemas.
pub(crate) mod value {
    pub const AUTO: &str = "auto";
    pub const DEFAULT: &str = "default";
    pub const NONE: &str = "none";
    pub const TEXT: &str = "text";
    pub const IMAGE: &str = "image";
    pub const OBJECT: &str = "object";
    pub const NULL: &str = "null";
    pub const STRING: &str = "string";
    pub const COMPLETED: &str = "completed";
    pub const IN_PROGRESS: &str = "in_progress";
    pub const MODEL: &str = "model";
    pub const RESPONSE: &str = "response";
}

/// JSON field names shared by payload construction, inspection, and catalog projection.
/// Serde attributes retain literals because its derive macros require them.
pub(crate) mod field {
    pub const ACCESS_PROGRAMS: &str = "access_programs";
    pub const ADDITIONAL_SPEED_TIERS: &str = "additional_speed_tiers";
    pub const ALL_OF: &str = "allOf";
    pub const ANY_OF: &str = "anyOf";
    pub const AUTHOR: &str = "author";
    pub const AVAILABLE_ACCESS_PROGRAMS: &str = "available_access_programs";
    pub const BASE_INSTRUCTIONS: &str = "base_instructions";
    pub const CALL_ID: &str = "call_id";
    pub const CAPABILITIES: &str = "capabilities";
    pub const CODE: &str = "code";
    pub const CONST: &str = "const";
    pub const CONTENT: &str = "content";
    pub const CONTEXT_WINDOW: &str = "context_window";
    pub const CREATED: &str = "created";
    pub const CREATED_AT: &str = "created_at";
    pub const DATA: &str = "data";
    pub const DEFAULT: &str = "default";
    pub const DEFAULT_REASONING_LEVEL: &str = "default_reasoning_level";
    pub const DEFAULT_REASONING_SUMMARY: &str = "default_reasoning_summary";
    pub const DEFAULT_SERVICE_TIER: &str = "default_service_tier";
    pub const DEFAULT_VERBOSITY: &str = "default_verbosity";
    pub const DEPRECATED: &str = "deprecated";
    pub const DESCRIPTION: &str = "description";
    pub const DETAIL: &str = "detail";
    pub const DISPLAY_NAME: &str = "display_name";
    pub const EFFORT: &str = "effort";
    pub const ENCRYPTED: &str = "encrypted";
    pub const ENCRYPTED_CONTENT: &str = "encrypted_content";
    pub const ENCRYPTED_FUNCTION_ARGS: &str = "encrypted_function_args";
    pub const ENUM: &str = "enum";
    pub const ERROR: &str = "error";
    pub const EXAMPLE: &str = "example";
    pub const EXAMPLES: &str = "examples";
    pub const FIRST_ID: &str = "first_id";
    pub const GET: &str = "get";
    pub const HAS_IMAGE_UNDERSTANDING: &str = "has_image_understanding";
    pub const HAS_MORE: &str = "has_more";
    pub const HISTORY_NOTES: &str = "history_notes";
    pub const ID: &str = "id";
    pub const IMAGES: &str = "images";
    pub const IMAGE_DETAIL_ORIGINAL: &str = "image_detail_original";
    pub const INCOMPLETE_DETAILS: &str = "incomplete_details";
    pub const INPUT: &str = "input";
    pub const INPUT_MODALITIES: &str = "input_modalities";
    pub const INSTRUCTIONS: &str = "instructions";
    pub const INSTRUCTIONS_TEMPLATE: &str = "instructions_template";
    pub const INTERNAL_CHAT_MESSAGE_METADATA_PASSTHROUGH: &str =
        "internal_chat_message_metadata_passthrough";
    pub const ITEM: &str = "item";
    pub const ITEMS: &str = "items";
    pub const LAST_ID: &str = "last_id";
    pub const LEVEL: &str = "level";
    pub const MAX_CONTEXT_WINDOW: &str = "max_context_window";
    pub const MAX_MODEL_LEN: &str = "max_model_len";
    pub const MAX_OUTPUT_TOKENS: &str = "max_output_tokens";
    pub const MESSAGE: &str = "message";
    pub const MODEL: &str = "model";
    pub const MODELS: &str = "models";
    pub const MODEL_INFO: &str = "model_info";
    pub const MODEL_MESSAGES: &str = "model_messages";
    pub const MODEL_PATH: &str = "model_path";
    pub const NAME: &str = "name";
    pub const NAMESPACE: &str = "namespace";
    pub const NOT: &str = "not";
    pub const OBJECT: &str = "object";
    pub const ONE_OF: &str = "oneOf";
    pub const OUTPUT: &str = "output";
    pub const OWNED_BY: &str = "owned_by";
    pub const PARAMETERS: &str = "parameters";
    pub const PATHS: &str = "paths";
    pub const POST: &str = "post";
    pub const PREFIX_ITEMS: &str = "prefixItems";
    pub const PREVIOUS_RESPONSE_ID: &str = "previous_response_id";
    pub const PROPERTIES: &str = "properties";
    pub const READ_ONLY: &str = "readOnly";
    pub const REASONING: &str = "reasoning";
    pub const REASONING_LEVEL: &str = "reasoning_level";
    pub const REASONING_LEVELS: &str = "reasoning_levels";
    pub const REASONING_SUMMARY: &str = "reasoning_summary";
    pub const RECIPIENT: &str = "recipient";
    pub const REF: &str = "$ref";
    pub const RESPONSE: &str = "response";
    pub const RESPONSES_LITE: &str = "responses_lite";
    pub const ROLE: &str = "role";
    pub const SEQUENCE_NUMBER: &str = "sequence_number";
    pub const SERVED_MODEL_NAME: &str = "served_model_name";
    pub const SERVICE_TIER: &str = "service_tier";
    pub const SERVICE_TIERS: &str = "service_tiers";
    pub const SLUG: &str = "slug";
    pub const STATUS: &str = "status";
    pub const STREAM: &str = "stream";
    pub const STREAM_ID: &str = "stream_id";
    pub const SUMMARY: &str = "summary";
    pub const SUPPORTED_REASONING_LEVELS: &str = "supported_reasoning_levels";
    pub const SUPPORTS_EXPERIMENTAL_CONTEXT: &str = "supports_experimental_context";
    pub const SUPPORTS_IMAGE_DETAIL_ORIGINAL: &str = "supports_image_detail_original";
    pub const SUPPORTS_REASONING_SUMMARY_PARAMETER: &str = "supports_reasoning_summary_parameter";
    pub const SUPPORTS_RESPONSES_LITE: &str = "supports_responses_lite";
    pub const SUPPORTS_SEARCH_TOOL: &str = "supports_search_tool";
    pub const SUPPORTS_TOOL_INVENTORY: &str = "supports_tool_inventory";
    pub const SUPPORT_VERBOSITY: &str = "support_verbosity";
    pub const TEXT: &str = "text";
    pub const TOOL_SEARCH: &str = "tool_search";
    pub const TITLE: &str = "title";
    pub const TOOLS: &str = "tools";
    pub const TOOL_INVENTORY: &str = "tool_inventory";
    pub const TYPE: &str = "type";
    pub const USAGE: &str = "usage";
    pub const USE_RESPONSES_LITE: &str = "use_responses_lite";
    pub const WEB_SEARCH: &str = "web_search";
    pub const VERBOSITY: &str = "verbosity";
    pub const WEB_SEARCH_TOOL_TYPE: &str = "web_search_tool_type";
    pub const WRITE_ONLY: &str = "writeOnly";
}

/// Responses item, content, and tool discriminators.
pub(crate) mod item_type {
    pub const ADDITIONAL_TOOLS: &str = "additional_tools";
    pub const AGENT_MESSAGE: &str = "agent_message";
    pub const COMPACTION: &str = "compaction";
    pub const COMPACTION_TRIGGER: &str = "compaction_trigger";
    pub const CUSTOM_TOOL_CALL: &str = "custom_tool_call";
    pub const ENCRYPTED_CONTENT: &str = "encrypted_content";
    pub const FUNCTION_CALL: &str = "function_call";
    pub const INPUT_IMAGE: &str = "input_image";
    pub const INPUT_TEXT: &str = "input_text";
    pub const INSTRUCTIONS: &str = "instructions";
    pub const MESSAGE: &str = "message";
    pub const NAMESPACE: &str = "namespace";
    pub const OUTPUT_TEXT: &str = "output_text";
    pub const REASONING: &str = "reasoning";
    pub const REFUSAL: &str = "refusal";
    pub const TOOL_SEARCH: &str = "tool_search";
    pub const TOOL_SEARCH_OUTPUT: &str = "tool_search_output";
    pub const WEB_SEARCH: &str = "web_search";
}

/// Message roles shared by history adaptation and capability probes.
pub(crate) mod role {
    pub const ASSISTANT: &str = "assistant";
    pub const DEVELOPER: &str = "developer";
    pub const SYSTEM: &str = "system";
    pub const USER: &str = "user";
}
