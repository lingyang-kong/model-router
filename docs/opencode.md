# OpenCode

Use the `/responses` rule from [router-config.toml](../examples/router-config.toml) with your Responses-compatible upstreams and credentials. Register custom IDs in provider `models` tables or file catalogs as described in [catalog configuration](../README.md#catalogs), so routing does not depend on a client catalog request. OpenCode manages its own picker.

For auxiliary requests, configure `[clients.openai].provider` and keep the example's catch-all `forward` rule after the catalog and model rules:

```toml
[clients.openai]
provider = "sglang"

# Last routing rule; omit this if the example's catch-all is already present.
[[routes]]
action = "forward"
```

Replace `sglang` with the configured provider that should handle OpenCode's auxiliary requests and incoming credentials. The example config only sets the Codex client provider, so OpenCode needs its own profile. Auxiliary requests stream to this provider without model inspection; `/responses` still routes by model. Without the catch-all, unmatched requests return 404; with it but without a client provider, they return 400.

For OpenCode v2, merge this into an existing Responses provider:

```json
{
  "providers": {
    "openai": {
      "settings": {
        "baseURL": "http://localhost:4134",
        "transport": "websocket"
      },
      "models": {
        "vendor/model": {
          "headers": { "x-model": "vendor/model" }
        }
      }
    }
  }
}
```

Replace both occurrences of `vendor/model` with the native upstream ID; repeat the model entry for additional models. Each selected upstream must support Responses WebSockets. The header selects the destination during the handshake; reconnect to change providers.

For HTTP, use `"transport": "http"`; routing reads the body's `model`, so `x-model` is unnecessary. Provider authentication is configured separately from the endpoint override.

See the [OpenCode v2 provider reference](https://opencode.ai/v2/docs/providers) for supported transports and version-specific settings.
