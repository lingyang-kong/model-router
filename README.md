# Model router

A local HTTP/WebSocket proxy that routes requests by rules or native model IDs. Upstreams must implement the client's API; request bodies are forwarded unchanged except for configured or client-specific transformations such as the Codex multi-agent adapter.

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
| `MODEL_ROUTER_PORT` | Listener port; defaults to `localhost:4134`. |

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

Explicit capability flags, model-scoped image support, native tool inventories, and request schemas refine inherited Codex fields. Unknown capabilities inherit their donor settings. A tool declaration does not establish acceptance of its separate input/output item types. For enabled Responses Lite or tool search with unknown input-item support, each catalog refresh probes `base_url + /responses` with an `additional_tools` or `tool_search_output` input followed by an invalid sentinel, `max_output_tokens: 1`, and `store: false`. Only HTTP 400/422 explicitly reporting that tested input type as unsupported records a negative fact; other outcomes remain unknown. A loosely validating server could generate up to one token. Discovery calls time out after three seconds. Queries use provider credentials only on the provider's origin; explicitly configured external URLs are queried anonymously.

Optional `[providers.NAME.capabilities]` supplies provider-wide facts that discovery cannot establish. Boolean fields are `service_tiers`, `web_search`, `access_programs`, `history_notes`, `responses_lite`, `tool_search`, `tool_inventory`, `image_detail_original`, and `images`. Configured values take precedence over discovery; omission preserves discovered or inherited behavior. These describe capabilities, not native catalog fields. For example, `service_tiers = false` removes inherited service-tier choices and defaults. `history_notes` describes the experimental History/Notes protocol, independently of compaction. See [Codex metadata](docs/codex.md#model-metadata) for projection details.

Responses request schemas are authoritative for advertised service tiers and hosted web search. A closed `service_tier` enum replaces inherited tier choices; `auto` and `default` are implicit, and omission leaves the provider in control of its default. Generated tier labels use their IDs without inheriting OpenAI pricing or speed claims. Closed tool-type enums determine web-search availability. References are resolved at those request fields; unrelated nested fields do not count. Open schemas, unsupported compositions, and conflicting endpoints remain unknown. Explicit declarations and configured capabilities take precedence over schema deductions, which take precedence over template metadata.

Discovery also reads `reasoning.summary` and `text.verbosity` enums and string defaults, including nullable fields. Codex output uses these facts for summary support/default and verbosity support/default. Reasoning effort choices and their default come only from explicit shared settings or the template; the router neither discovers nor validates them against the provider schema. Unknown or conflicting declarations preserve the template; an open schema's missing property does not mean unsupported.

IDs explicitly configured under a provider’s `models` table are routable immediately, before remote discovery; they take precedence over the fallback. Other remote IDs are learned from catalog refreshes. A failed refresh keeps the previous discovered routing data and subagent listing; explicit configured ownership remains usable. IDs missing from a later refresh remain routable until restart but are excluded from both catalog responses and subagent listings. Before remote discovery succeeds, the client's cached model list is preserved; file-only catalogs are available at startup. Conflicting ownership rejects the refresh. The router preserves catalog envelopes and pagination fields, so use a complete catalog endpoint.

When a catalog route's provider has no configured `catalog` source, the router forwards the client's original method, path, query, and body to that provider as before. This allows a Codex route to fetch the official ChatGPT catalog and use an exact catalog entry as an inheritance donor.

Set `hidden_models = ["gpt-{5.6-*,6-sol}"]` on a provider to omit matching IDs from merged catalogs. Patterns use [globset syntax](https://docs.rs/globset/0.4.20/globset/): `*`, `?`, character classes such as `[0-9]` and `[!0-9]`, and brace alternatives such as `{sol,astra}` or `{,-preview}`. Backslashes escape metacharacters; use TOML literal strings such as `'model-\*'` to match a literal `*`.

Patterns match whole IDs case-sensitively, and `*` continues to match `/`. They are compiled once at startup; malformed patterns reject the configuration. Bash sequence expansion such as `{1..3}` and gitignore-style `!pattern` exceptions are not supported. Hidden models remain directly routable and still participate in ownership checks; this is a model-list preference, not access control. Forwarded catalogs are unchanged.

Each rule selects the source whose catalog envelope is retained when its format matches the output. File sources contribute model rows only. Additional remote sources receive their own configured headers and credentials, without client headers. The router never reads client cache files.

Recognized Codex `originator` values select Codex handling; other originators remain unadapted; no router-specific client header is needed. On HTTP and WebSocket Model routes it also enables the multi-agent v2 JSON transformations: rename collaboration tools upstream and restore their identities, remove the `message.encrypted` schema property from the three task functions, and map replayed tool identities. Unchanged payload bytes pass through; JSON payloads are rewritten only when an adaptation applies. Plaintext `agent_message` content relabeled as `input_text` remains marked Codex traffic; this relabeling does not decrypt `encrypted_content` or rewrite nested code-mode calls. For each actual input `agent_message`, the router validates support with a POST to the same provider, path, and model, using an invalid trailing sentinel, `max_output_tokens: 1`, and `store: false`. Only an explicit HTTP 400 or 422 reporting unsupported `agent_message` converts it to an ordinary user-message envelope; other outcomes preserve the native item. The probe runs on each applicable request, with no acceptance cache. Subagent model listings come from non-hidden models in the routing catalog, regardless of provider flags or whether a model is native or custom; they are learned during the existing catalog refresh. Codex WebSocket routes use the initial routing hint and reselect the provider from every model request. Inference requires a real upstream WebSocket handshake. An initial unsupported upgrade returns HTTP 426 so the client can choose HTTP fallback; a rejected upgrade after switching providers sends an error and closes the client socket. Seamless fallback between WebSocket and HTTP-only providers is not guaranteed. The adapted handshake does not negotiate `permessage-deflate`; generic raw WebSocket routes preserve extensions. Request compression may stay enabled. See [Codex setup and limits](docs/codex.md#multi-agent-v2-compatibility). Client/format mismatches and conflicting routing hints are rejected.

Codex compaction is handled when a normal Responses request contains an actual `compaction_trigger` input item. The router checks that provider's Responses endpoint on each trigger; it does not infer support from `/responses/compact` or probe ordinary requests, checkpoints, or text mentions. Supported providers handle compaction natively. When the endpoint explicitly rejects the trigger item with HTTP 400 or 422, the router generates a synthetic text summary; other probe outcomes leave handling native. Router-owned checkpoints are hydrated on later inference requests, while unknown opaque checkpoints pass through. See [Codex compaction behavior](docs/codex.md#remote-v2-compaction).

## Transport

The Codex adapter shares a conservative byte scan across WebSocket, HTTP JSON, and SSE before parsing relevant protocol envelopes. Messages without relevant protocol names skip adaptation parsing. Any JSON escape triggers the parsing path so escaped names cannot bypass rewriting. WebSocket framing, HTTP model selection, and decompression where required still apply.

Codex SSE framing is normalized to LF, with comments forwarded promptly. Rewritten JSON data loses literal CR/LF whitespace so it fits on one `data:` line; string contents and escaped newlines are preserved. Unmarked SSE streams pass through unchanged.

HTTP model routing buffers the request to read `model`; responses stream. Unchanged traffic preserves bytes and trailers. Merged catalogs use streamed gzip when accepted by the client, otherwise uncompressed JSON, independently of upstream encoding. Explicit encoding exclusions and an explicit preference for identity are respected; rejecting both supported choices returns HTTP 406. Valid provider headers remain, and `Vary: Accept-Encoding` is added when needed. Stale lengths, integrity fields, and cache validators are discarded because the merged body differs. The router adds no payload or concurrency cap.

Generic WebSocket model routes need `x-model: <native ID>` and tunnel bytes after the handshake. Codex uses its native routing hint, or the model route's fallback provider when no hint is available, for a real upstream handshake. Later model requests are routed independently; switching between providers with WebSocket support is supported. Clients own HTTP fallback when an upstream rejects WebSocket. See [Codex routing and fallback](docs/codex.md#websocket-routing).

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

A clean first build fetches the pinned Codex compaction prompt templates and license into Cargo's `OUT_DIR` using `curl`; it needs network access. Cargo's build cache avoids fetching them again on incremental builds. `cargo --offline` controls Cargo dependency resolution and does not provide uncached prompt files.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```
