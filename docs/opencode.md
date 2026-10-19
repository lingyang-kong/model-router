# OpenCode

Use the `/responses` rule from [router-config.toml](../examples/router-config.toml) with your Responses-compatible upstreams and credentials. Set `[clients.openai].provider` if OpenCode needs a default upstream or passes that provider's credentials through the router. Register custom IDs in provider `models` tables or file catalogs as described in [catalog configuration](../README.md#catalogs), so routing does not depend on a client catalog request. OpenCode manages its own picker.

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
