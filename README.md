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
| `model` | Route by the top-level JSON `model`; optional `provider` is the fallback for empty bodies, missing HTTP models, or unknown IDs. |
| `catalog` | Merge provider catalogs into the required `format`, using the required `provider` as the primary source. |

A model route's fallback also identifies the owner of incoming credentials. Routing from that provider to another `passthrough` provider is rejected; configure the destination's credentials on the router.

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

Source and output formats are independent. Use complete catalog endpoints: pagination is not implemented, and `has_more = true` is rejected. The primary source's envelope is retained when its format matches the output; file sources contribute rows only.

When the primary provider has no configured catalog source, the router fetches its catalog using the client's original method, path, query, and body. This lets a Codex catalog route use the authenticated ChatGPT catalog as an inheritance donor. Auxiliary sources receive their own provider credentials and headers, without client headers.

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
- When only the local source matches the output, local fields are retained and missing shared fields can come from the donor. If neither matches, the row is omitted from that output but its ID remains routable. OpenAI rows therefore need a Codex donor to appear in Codex output.

Additional metadata keys are merged into source-format rows: objects merge recursively, while arrays and scalars replace earlier values. Cross-format inheritance carries shared metadata rather than arbitrary local native fields. Metadata values are not semantically validated, but rows must have valid IDs and overrides cannot change a source ID.

### Refresh and ownership

Each catalog request refreshes all configured remote catalogs and optional discovery sources. There is no background refresh. IDs in explicit `models` tables and file catalogs are routable at startup; other remote IDs require a successful catalog request. A model-only route does not initiate discovery.

All sources, references, and ownership checks must succeed before a refresh is published. A catalog fetch failure, missing donor, or ownership conflict rejects the refresh and preserves the previous routing data and subagent listing. Optional discovery failures instead contribute no capability facts.

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

Merged catalogs use gzip when accepted, otherwise uncompressed JSON. Encoding exclusions and explicit identity preferences are respected; rejecting both supported encodings returns 406. Merged responses discard stale lengths, integrity fields, and cache validators, and vary on `Accept-Encoding`.

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
