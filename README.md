# Model router

A local HTTP/WebSocket proxy that routes requests by rules or native model IDs. Upstreams must implement the client's API; request bodies are forwarded unchanged.

## Run

Adapt [router-config.toml](examples/router-config.toml), then run from this directory:

```sh
MODEL_ROUTER_CONFIG=examples/router-config.toml cargo run --release --locked
```

The example connects a ChatGPT subscription and Qwen over SGLang. Client setup: [Codex](docs/codex.md), [Claude Code](docs/claude-code.md), [OpenCode](docs/opencode.md).

| Setting | Purpose |
| --- | --- |
| `MODEL_ROUTER_CONFIG` | TOML configuration file. |
| `MODEL_ROUTER_CONFIG_TOML` | Inline TOML when no file is selected. |
| `MODEL_ROUTER_PORT` | Listener port; defaults to `localhost:8080`. |

Relative catalog paths resolve beside the configuration file, or from the working directory for inline configuration. `~/` expands to the user's home directory.

## Providers and routes

Define providers with `[providers.<name>]` and rules with `[[routes]]`.

Each provider needs a `base_url` and `auth`:

| Authentication | Behavior |
| --- | --- |
| `passthrough` | Keep incoming credentials. |
| `none` | Strip known client credential headers. |
| `bearer` | Strip those headers and add a bearer token from the required `env_key`. |

Only `bearer` accepts `env_key`. Optional `http_headers` and `env_http_headers` add literal or environment-sourced headers after authentication handling.

Rules can match `method`, `path` or `path_prefix`, `headers`, and `query`. All conditions must match; the first matching rule wins. Header names are case-insensitive; values are exact. Unmatched requests return 404.

| Action | Behavior |
| --- | --- |
| `forward` | Stream to the named `provider`. |
| `model` | Route by the top-level JSON `model`; optional `provider` handles empty bodies, missing HTTP models, or unknown IDs. |
| `catalog` | Fetch from `provider` and merge discovered provider models using the required output `format`. |

A model route's fallback also identifies whose credentials arrived. Routing to another `passthrough` provider is rejected; configure that destination's credentials on the router.

The full incoming path is appended to `base_url`: `/v1` plus `/responses` becomes `/v1/responses`. Paths changed by URL normalization (such as dot segments or backslashes) are rejected. Incoming queries are preserved; base URLs cannot contain queries.

## Catalogs

Provider catalogs can come from a remote endpoint or a JSON file. A remote OpenAI-compatible `/v1/models` endpoint can be configured as:

```toml
catalog = { path = "/models", format = "openai" }
```

The path is appended to the provider's `base_url`; for the example, SGLang is queried at `http://localhost:30000/v1/models`. A file source remains available, for example `catalog = { file = "models.json", format = "openai" }`. Read either source format independently of the catalog route's output format.

| Format | Model array | Required ID field |
| --- | --- | --- |
| `codex` | `models` | `slug` |
| `openai` | `data` | `id` |
| `anthropic` | `data` | `id` |

Remote catalogs are refreshed on every catalog request, including the tools endpoint. The request uses the source provider's authentication and configured headers. File sources are read at startup and require a router restart after edits. Catalog inheritance copies a complete entry from another provider's current catalog, then applies local shared settings.

Shared model metadata can fill or replace shared fields:

```toml
[providers.sglang.models."vendor/model"]
name = "My coding model"
description = "A coding assistant"
extends = { provider = "chatgpt", model = "gpt-6-luna" }
reasoning_levels = [
  { level = "low", description = "Quick responses" },
  { level = "high", description = "More reasoning" },
]
# reasoning_level = "high" # Optional default
```

Use `[providers.NAME.defaults]` and `[providers.NAME.models."native/id"]` for shared settings. `name`, `description`, `reasoning_level`, and `reasoning_levels` keep the same neutral names regardless of catalog format. `extends` identifies a provider and its exact native model ID separately; `/` inside a model ID is never interpreted as a provider prefix. The referenced entry is copied in full, then local shared settings are applied. Omitted description and reasoning settings inherit; an omitted name falls back to the local model ID in Codex output. Provider endpoint and authentication settings are never inherited. Generic native-field overrides are deferred; inherited native fields currently pass through unchanged.

When returning the local source format with a different-format donor, inherit shared donor metadata and retain the local source fields. If neither format matches the output, omit that model while retaining its discovered routing ID. OpenAI source `max_model_len` supplies Codex `max_context_window` when present. Metadata values are not semantically validated. Structural and routing-ID checks still apply. Inference requests are forwarded unchanged.

Inheritance is resolved directly from the catalogs fetched in the same refresh. Donor shared settings are included; the donor's own `extends` is not followed. A missing donor, unknown provider, or provider mismatch rejects the refresh and keeps the last successful routing data. Hidden donor entries remain available as inheritance sources. OpenAI rows without `extends` are not automatically added to Codex output. A provider default may contain `extends` to apply one base entry to all of that provider's models.

Optional `discovery_urls` on a provider supply additional HTTP JSON information, such as model info, OpenAPI, and tool inventories. Every catalog request fetches these sources again; observations are not cached across requests. The router also follows same-origin tool-list GET paths advertised in OpenAPI. URLs are deduplicated within that refresh. A fresh negative tool-inventory declaration, provider-wide or covering every listed model, skips tool-list queries for that refresh.

Explicit capability flags, model-scoped image support, native tool inventories, and closed image-detail enums can refine inherited Codex fields. Unknown capabilities inherit their donor settings. A fresh negative tool-inventory declaration suppresses tool-list queries. A tool type appearing in an OpenAPI enum alone does not establish runtime support. If a Codex-projected row enables Responses Lite and discovery has not declared support, the router optionally POSTs to `base_url + /responses` with `input` containing `[{"type":"additional_tools","role":"developer","tools":[]},{"type":"__model_router_capability_check__"}]`, `max_output_tokens: 1`, and `store: false`. Only an HTTP 400 or 422 whose error message begins `Unsupported Responses API input item type: 'additional_tools'` records unsupported; every other outcome remains unknown. The sentinel is intended to trigger validation, though a loosely validating server could generate up to one token. Discovery calls time out after three seconds. Queries use provider credentials only on the provider's origin; explicitly configured external URLs are queried anonymously.

Remote model IDs are unknown to model routing until the first successful catalog refresh unless a fallback provider is configured. A failed refresh keeps the previous routing data. IDs missing from a later refresh remain routable until restart but are no longer displayed. Conflicting ownership rejects the refresh. The router preserves catalog envelopes and pagination fields, so use a complete catalog endpoint.

When a catalog route's provider has no configured `catalog` source, the router forwards the client's original method, path, query, and body to that provider as before. This allows a Codex route to fetch the official ChatGPT catalog and use an exact catalog entry as an inheritance donor.

Set `hidden_models = ["gpt-{5.6-*,6-sol}"]` on a provider to omit matching IDs from merged catalogs. Patterns use [globset syntax](https://docs.rs/globset/0.4.20/globset/): `*`, `?`, character classes such as `[0-9]` and `[!0-9]`, and brace alternatives such as `{sol,astra}` or `{,-preview}`. Backslashes escape metacharacters; use TOML literal strings such as `'model-\*'` to match a literal `*`.

Patterns match whole IDs case-sensitively, and `*` continues to match `/`. They are compiled once at startup; malformed patterns reject the configuration. Bash sequence expansion such as `{1..3}` and gitignore-style `!pattern` exceptions are not supported. Hidden models remain directly routable and still participate in ownership checks; this is a model-list preference, not access control. Forwarded catalogs are unchanged.

Each rule selects the source whose catalog envelope is retained when its format matches the output. File sources contribute model rows only. Additional remote sources receive their own configured headers and credentials, without client headers. The router never reads client cache files.

`x-router-client: codex` enables Codex's v1 metadata adjustment and WebSocket hint parser. Omit it for other clients. Unknown or conflicting markers and client/format mismatches are rejected.

## Transport

HTTP model routing buffers the request to read `model`; responses stream. Unchanged traffic preserves bytes and trailers. Rewritten catalogs discard stale encoding, integrity, and cache validators. The router adds no payload or concurrency cap.

WebSocket model routes need `x-model: <native ID>` or a [Codex routing hint](docs/codex.md#websockets). After the handshake, bytes are tunneled without inspecting messages. Reconnect to change providers.

Loopback upstreams bypass proxy environment variables; remote upstreams honor them.

## systemd

Adapt the paths and credentials in [model-router.service](model-router.service), set `User=` if needed, and install it under `/etc/systemd/system/`:

```sh
sudo systemctl daemon-reload
sudo systemctl start model-router
```

SIGINT and SIGTERM drain in-flight requests before shutdown.

## Development

See the [feature plan](docs/roadmap.md) for discovery, aliases, and optional client compatibility work.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```
