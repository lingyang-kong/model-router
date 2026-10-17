# OpenCode

Adapt the `/responses` routing rule from [router-config.toml](../examples/router-config.toml) for your Responses-compatible upstreams and credentials. That file demonstrates ChatGPT/SGLang with Codex; OpenCode needs its own provider configuration below. Register custom model IDs in a manual catalog. Omit `x-router-client`; OpenCode manages its own model picker.

For OpenCode v2 WebSockets, merge this into an existing Responses provider:

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

Replace both occurrences of `vendor/model` with the native upstream ID; repeat the model entry for additional models. All selected upstreams must support Responses WebSockets. The header selects the destination during the handshake; reconnect to change providers. HTTP requests use the body's `model` field.

[OpenCode v2 documents these settings](https://opencode.ai/v2/docs/providers), but transport activation depends on the installed build. In the audited source (`388406238bd5ca15564a762840a2362c3a45bd9c`), WebSocket construction accepts model headers while the native session adapter still selects HTTP. Older versions use different settings.
