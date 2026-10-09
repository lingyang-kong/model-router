# OpenCode

Use a per-model `x-model` header containing the exact native upstream model ID. The router's `model` action resolves it through the same catalog map as HTTP requests. HTTP continues to use the JSON body; no model alias is needed.

For OpenCode v2, merge this into an existing Responses provider configuration:

```json
{
	"providers": {
		"openai": {
			"settings": {
				"baseURL": "http://localhost:8080",
				"transport": "websocket"
			},
			"models": {
				"gpt-6.1-sol": {
					"headers": { "x-model": "gpt-6.1-sol" }
				},
				"vendor/model": {
					"headers": { "x-model": "vendor/model" }
				}
			}
		}
	}
}
```

Reuse the router's `/responses` model route from the [Codex example](codex.md), with credentials appropriate to your upstreams. Add inference IDs to the manual catalog. The selected model's header must name that same upstream ID. Provider bases must align with the full path sent by the client, and each selected upstream must implement Responses WebSockets.

[OpenCode v2 documents per-model headers and WebSocket transport](https://opencode.ai/v2/docs/providers). The audited source at `388406238bd5ca15564a762840a2362c3a45bd9c` passes model headers to the WebSocket constructor when that transport is used, but its native session adapter still selects the HTTP route. Transport activation therefore depends on the installed build. Older versions use different configuration keys and transport switches.

The hint is sent during the handshake. An existing connection cannot change its destination; changing providers requires a new connection. Session-affinity headers identify sessions and do not identify models.
