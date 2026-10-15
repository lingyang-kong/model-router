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
| `catalog` | Fetch from `provider` and merge manual entries using the required `format`. |

A model route's fallback also identifies whose credentials arrived. Routing to another `passthrough` provider is rejected; configure that destination's credentials on the router.

The full incoming path is appended to `base_url`: `/v1` plus `/responses` becomes `/v1/responses`. Paths changed by URL normalization (such as dot segments or backslashes) are rejected. Incoming queries are preserved; base URLs cannot contain queries.

## Catalogs

Manual catalogs are JSON files, configured on a provider. For a Codex catalog:

```toml
catalog = { file = "codex-model-catalog.json", format = "codex" }
```

| Format | Model array | Required ID field |
| --- | --- | --- |
| `codex` | `models` | `slug` |
| `openai` | `data` | `id` |
| `anthropic` | `data` | `id` |

Supply the client's required metadata. Only matching formats are merged; all manual IDs remain routable. Restart after manual edits.

Catalog rules share one live provider, which cannot also have a manual catalog. Refreshes learn new IDs; missing IDs remain routable until restart. Conflicting ownership rejects the whole refresh. The router never reads client cache files. It preserves catalog envelopes and pagination fields, so use a complete catalog endpoint.

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

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```
