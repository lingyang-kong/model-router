# Model router

A local HTTP/WebSocket proxy that routes requests by rules or native model IDs. Upstreams must implement the client's API; the router does not translate inference APIs. Codex adapters provide catalog inheritance with identity rewriting, capability discovery, multi-agent compatibility, and compaction fallback.

## Run

Adapt [router-config.toml](examples/router-config.toml), then run from this directory:

```sh
MODEL_ROUTER_CONFIG=examples/router-config.toml cargo run --release --locked
```

The example connects a ChatGPT subscription and SGLang. Client setup: [Codex](docs/codex.md), [Claude Code](docs/claude-code.md), [OpenCode](docs/opencode.md).

Builds require Rust with edition 2024 support. A clean first build also needs `curl` and network access: [build.rs](build.rs) fetches compaction prompts and their license from a pinned Codex revision into Cargo's `OUT_DIR`. Incremental builds reuse them; `cargo --offline` does not supply missing prompt files.

| Setting | Purpose |
| --- | --- |
| `MODEL_ROUTER_CONFIG` | TOML configuration file; takes precedence over inline configuration. |
| `MODEL_ROUTER_CONFIG_TOML` | Inline TOML when no file is selected. |
| `MODEL_ROUTER_PORT` | Listener port; defaults to `4134` on `localhost`. |

The listener is unauthenticated and binds to loopback. Keep access restricted to trusted local clients, especially when the router holds provider credentials.

## Providers and routes

Define providers with `[providers.NAME]`. Each requires `base_url` and `auth`:

| Authentication | Behavior |
| --- | --- |
| `passthrough` | Keep incoming credentials. |
| `none` | Strip known client credential headers. |
| `bearer` | Strip those headers and add a bearer token from the required `env_key`. |

Only `bearer` accepts `env_key`. `http_headers` and `env_http_headers` add literal or environment-sourced headers after authentication handling. Keep secrets in environment variables rather than committed configuration. Credential stripping covers authorization, cookies, API keys, OpenAI organization/project headers, and `chatgpt-`/`x-openai-` headers; it is not an allowlist of all forwarded headers.

Define rules with `[[routes]]`. Rules can match `method`, `path` or `path_prefix`, `headers`, and `query`. All conditions must match; the first matching rule wins. Header names are case-insensitive, values are exact, and unmatched requests return 404.

| Action | Behavior |
| --- | --- |
| `forward` | Stream to the required `provider`. |
| `model` | Route by the top-level JSON `model`, using the client's configured provider as a fallback. |
| `catalog` | Merge all configured provider catalogs in the client's format. |

Provider and output format belong to clients, independently of endpoint routing:

```toml
[clients.codex]
provider = "chatgpt"
format = "codex"

[[routes]]
method = "GET"
path = "/models"
action = "catalog"
```

Supported client profiles are `codex`, `anthropic`, and `openai`. A Codex `originator` header selects `codex`; otherwise `anthropic-version` selects `anthropic`; other requests select `openai`. Profiles are optional. Their `format` defaults to the client's name, and `provider` has no default. For example, an unconfigured OpenAI client gets an OpenAI catalog and can route known model IDs without using ChatGPT credentials.

A client's provider identifies the owner of incoming credentials and supplies fallback routing for empty bodies, missing HTTP models, or unknown IDs. Routing inference from that provider to another `passthrough` provider is rejected; configure the destination's credentials on the router. Catalog and model routes do not accept `provider` or `format`; explicit `forward` routes still name their destination.

The full incoming path is appended to `base_url`: a base ending in `/v1` plus `/responses` becomes `/v1/responses`. Incoming queries are preserved; base URLs cannot contain queries. Paths changed by URL normalization, such as dot segments or backslashes, are rejected. Upstream redirects are not followed. Loopback upstreams bypass proxy environment variables; remote upstreams honor them.

## Catalogs

Each provider can use one remote endpoint or JSON file:

```toml
catalog = { path = "/models", format = "openai" }
# Or: catalog = { file = "models.json", format = "openai" }
```

Endpoint paths are appended to `base_url` and cannot contain queries. Relative file paths resolve beside the configuration file, or from the working directory for inline configuration; `~/` expands to the user's home directory. Files are read at startup and require a restart after edits.

| Format | Model array | Required ID field |
| --- | --- | --- |
| `codex` | `models` | `slug` |
| `openai` | `data` | `id` |
| `anthropic` | `data` | `id` |

Source and output formats are independent. Each contributing provider declares its catalog path or file and source format. The selected client profile determines the output format, so one `/models` route serves all clients.

The response retains the envelope and headers of the first successful remote source matching the output format, in provider-name order. If none matches, it uses a standard envelope and fresh headers. File sources contribute rows only. Use complete catalog endpoints: upstream pagination is not implemented, and `has_more = true` is rejected. Anthropic `first_id`, `last_id`, and `has_more` describe the final merged list.

Only the selected client's provider receives incoming credentials, headers, and query parameters during catalog fetching. Every remote catalog uses its configured path and a GET request with an empty body. Other sources receive only their own configured credentials and headers; a passthrough source without matching client ownership is queried anonymously. Providers without a catalog source do not participate in catalog refreshes.

### Metadata and inheritance

Use `[providers.NAME.defaults]` for defaults and `[providers.NAME.models."native/id"]` for per-model settings:

```toml
[providers.sglang.models."vendor/model"]
extends = { provider = "chatgpt", model = "gpt-6-luna" }
name = "My coding model"
description = "A coding assistant"
reasoning_level = "high"
reasoning_levels = [
  { level = "low", description = "Quick responses" },
  { level = "high", description = "More reasoning" },
]
```

`name`, `description`, `reasoning_level`, and `reasoning_levels` are shared settings mapped to each catalog format. Provider defaults fill missing source fields; per-model settings override source values. Reasoning choices are not discovered or checked against the provider schema, so configure values the model actually accepts.

`extends` names a provider and exact native model ID separately; `/` inside an ID is never a provider prefix. It can also appear in provider defaults. Inheritance follows these rules:

- References resolve against the same refresh, including hidden donor rows and donor shared settings. The donor's own `extends` is not followed.
- When the donor matches the output format, its complete row is copied and local shared settings are applied. Same-format local rows also retain their native fields. Provider endpoints and authentication are never inherited.
- Omitted descriptions and reasoning settings inherit. In Codex output, an omitted local name becomes the local model ID; inherited instruction identities are rewritten as described in [Codex metadata](docs/codex.md#model-metadata).
- When only the local source matches the output, local fields are retained and missing shared fields can come from the donor. If neither matches, OpenAI and Anthropic output use the model ID and shared metadata to build a basic row; opaque native fields are not copied. Synthesized OpenAI rows use the configured provider name for `owned_by`, and synthesized timestamps use the Unix epoch to represent an unknown release time. Codex output still needs a native Codex row or donor; otherwise the row is omitted but its ID remains routable.

Additional metadata keys are merged into source-format rows: objects merge recursively, while arrays and scalars replace earlier values. Cross-format inheritance carries shared metadata rather than arbitrary local native fields. Metadata values are not semantically validated, but rows must have valid IDs and overrides cannot change a source ID.

### Refresh and ownership

Each catalog request refreshes all configured remote catalogs and optional discovery sources. There is no background refresh. IDs in explicit `models` tables and file catalogs are routable at startup; other remote IDs require a successful catalog request. A model-only route does not initiate discovery.

Catalogs are merged on a best-effort basis: transport failures and non-200 responses omit that source, while successful remote catalogs (including empty ones) and file catalogs still contribute. Each remote catalog request has a three-second timeout covering connection, response headers, and body transfer. Models whose inheritance donor's source is unavailable are omitted from Codex output and subagent listings; OpenAI and Anthropic output can still describe them using their own source metadata. If every catalog source fails, the client's provider supplies the error when it has a catalog source; otherwise the first source in provider-name order supplies it. The previous routing data and subagent listing are preserved.

Successful partial refreshes update the catalog and subagent listing together; unavailable models retain their known routes but are no longer advertised. Invalid catalog data, missing donors from successful sources, and ownership conflicts still reject the refresh without publishing changes. Optional discovery failures contribute no capability facts.

IDs removed by a later successful refresh remain owned and routable until restart, but are no longer advertised. Hidden models likewise remain routable and participate in ownership checks. The router never reads client cache files.

### Hidden models

Set `hidden_models = ["gpt-{5.6-*,6-sol}"]` on a provider to omit matching IDs from merged catalogs and subagent listings. This is a display preference, not access control; `forward` catalogs are unchanged.

Patterns match whole IDs case-sensitively, and `*` matches `/`. Supported [globset syntax](https://docs.rs/globset/0.4.20/globset/) includes `*`, `?`, character classes, and brace alternatives. Use TOML literal strings to preserve escapes, such as `'model-\*'` for a literal `*`. Bash sequences (`{1..3}`) and gitignore exclusions (`!pattern`) are unsupported. Malformed patterns reject configuration at startup.

## Capability discovery

Optional provider `discovery_urls` supply HTTP JSON documents such as model information, OpenAPI schemas, or tool inventories. The router also follows advertised same-origin tool-list GET paths. URLs are deduplicated within a refresh; observations are not cached across requests. A provider-wide negative inventory declaration, or negatives covering every listed model, skips tool-list queries for that refresh.

Optional discovery calls time out after three seconds. Provider credentials are sent only to the provider's origin; explicitly configured external URLs are queried anonymously. Unavailable or inconclusive sources leave capabilities unknown, preserving inherited metadata. These timeouts do not bound the complete catalog refresh.

Override provider-wide facts with `[providers.NAME.capabilities]`. Supported booleans are `service_tiers`, `web_search`, `access_programs`, `history_notes`, `responses_lite`, `tool_search`, `tool_inventory`, `image_detail_original`, `images`, and `reasoning_summary`. For example:

```toml
[providers.sglang.capabilities]
access_programs = false
history_notes = false
```

Configured values take precedence over discovered facts. Model-scoped observations override provider-wide observations, and source-row flags are reapplied before configured overrides. Across discovery documents, later known values overwrite earlier ones, so URL order can matter. Within a schema document, unsupported or conflicting deductions remain unknown.

Discovery refines catalog metadata; it does not translate inference requests or prove that every advertised feature works. Codex output may also send inference-endpoint [capability probes](docs/codex.md#capability-probes). See [Codex metadata](docs/codex.md#model-metadata) for field mappings and limitations.

## Transport

HTTP model routing buffers requests to read `model`. Forward routes stream requests; inference responses stream except for Codex JSON responses that require inspection. Unchanged traffic preserves payload bytes and trailers. The router adds no payload or concurrency cap.

Merged catalogs use gzip when accepted, otherwise uncompressed JSON. Encoding exclusions and explicit identity preferences are respected; rejecting both supported encodings returns 406. Merged responses discard stale lengths, integrity fields, and cache validators, and vary on `Accept-Encoding`, `Originator`, and `Anthropic-Version`.

Generic WebSocket model routes require `x-model: <native ID>` and tunnel bytes after the handshake. Reconnect to change providers. [Codex WebSocket routing](docs/codex.md#websocket-routing) supports per-request selection and has separate fallback behavior. Clients own fallback to HTTP when an upstream rejects WebSocket.

## Deployment

Adapt the paths, runtime credentials, and service user in [model-router.service](model-router.service), then install it under `/etc/systemd/system/`:

```sh
sudo systemctl daemon-reload
sudo systemctl start model-router
```

The supplied unit runs `cargo run`, so startup may build the project and needs the build prerequisites above. For a prebuilt deployment, run `cargo build --release --locked` first and set `ExecStart` to the absolute path of `target/release/model-router`. SIGINT and SIGTERM initiate graceful HTTP shutdown.

## Development

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

The main boundaries are routing/authentication in `router.rs` and `config.rs`, catalog snapshots and projection in `catalog.rs` and `adapters/catalog.rs`, evidence collection in `discovery.rs` and `capabilities.rs`, and Codex protocol adaptation under `adapters/codex/`. Keep source metadata separate from client projections, publish routing changes atomically, and preserve opaque payloads outside the protocol fields being adapted.
