# Model router

A local HTTP/WebSocket proxy that selects upstream providers from routing rules or native model IDs. Providers must implement the API their clients send; the router preserves request bodies and does not translate between APIs.

## Run

For Codex with ChatGPT sign-in and custom models, follow the [three-step Codex setup](docs/codex.md).

From the repository directory, adapt the [router configuration](examples/router-config.json) and [Codex model catalog](examples/codex-model-catalog.json), then:

```sh
export MODEL_ROUTER_CONFIG=examples/router-config.json
cargo run --release --locked
```

The listener binds to `localhost:8080`. Set `MODEL_ROUTER_PORT` to change the port. `MODEL_ROUTER_CONFIG_JSON` accepts inline configuration when no file is selected. Relative catalog paths resolve beside the configuration file, or from the working directory for inline configuration. `~/` expands to the user's home directory. Use `model-router --help` or `--version` without loading configuration.

## Configuration

The configuration contains a `providers` map and ordered `routes`. See the [complete example](examples/router-config.json).

Each provider has a `base_url` and an `auth` mode:

- `passthrough`: retain incoming credentials.
- `none`: strip known client credential headers.
- `bearer`: strip those headers and add a bearer token from `env_key`.

`http_headers` adds literal headers; `env_http_headers` reads header values from named environment variables. These are applied after the authentication mode. `catalog_file` supplies a manually managed model catalog.

Routes match any combination of `method`, `path` or `path_prefix`, `headers`, and `query`. All conditions must match; the first matching route wins. Header names are case-insensitive; values are exact. Unmatched requests return 404.

| Action | Behavior |
| --- | --- |
| `forward` | Stream immediately to the named `provider`. |
| `model` | Select through the model registry; optional `provider` is the fallback for missing HTTP models or unknown IDs. |
| `catalog` | Forward to the named provider and merge manual entries into a successful catalog response. |

A model route's fallback also identifies the incoming credentials' provider. Cross-provider routing to another passthrough destination is rejected; configure that destination's credentials on the router instead.

The complete incoming path is appended to `base_url`: a base ending in `/v1` plus `/responses` becomes `/v1/responses`. Incoming queries are preserved; base URLs must not contain queries. Native model IDs are never rewritten.

## Catalogs and streaming

Manual catalogs contain a `models` or `data` array. `models` prefers `slug` and `data` prefers `id`; the other field is accepted when the preferred one is absent. Entries must contain the client's required metadata. Restart after manual edits.

All catalog routes share one live provider, which has no manual `catalog_file`. Refreshes return current upstream metadata plus manual entries and remember newly seen IDs for routing. Missing IDs remain routable until restart. Conflicting ownership rejects the whole refresh. Harness-managed cache files are never used. Catalog envelopes are preserved, including counts and pagination fields; use a complete catalog endpoint rather than a paginated one.

HTTP model routing buffers the request to read its top-level `model`. Responses stream, and unchanged traffic retains its bytes and trailers. Rewritten catalogs drop obsolete encoding, integrity, and validator headers and trailers. The router adds no payload or concurrency cap; transport-library defaults still apply.

WebSocket model routes require a handshake hint:

- `x-codex-routing-hint: model=<native ID>;tier=<optional tier>`
- `x-model: <native ID>`

The upstream handshake is forwarded, then bytes are tunneled without inspecting messages. Reconnect to select another provider. Loopback upstreams bypass proxy environment variables; remote upstreams honor them.

## Clients

- [Codex](docs/codex.md)
- [Claude Code](docs/claude-code.md)
- [OpenCode](docs/opencode.md)

## systemd

[model-router.service](model-router.service) is a system-unit template. Adapt its absolute paths, configuration, and credential environment for the deployment. After installing it in `/etc/systemd/system/`, reload the manager and start it:

```sh
sudo systemctl daemon-reload
sudo systemctl start model-router
```

Both SIGINT and SIGTERM initiate graceful shutdown. The unit runs under the system manager's default account unless `User=` is configured.

## Development

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```
