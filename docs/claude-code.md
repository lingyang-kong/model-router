# Claude Code

## API compatibility

Set Claude Code's API root to the router:

```sh
export ANTHROPIC_BASE_URL="http://localhost:8080"
claude
```

The client sends `/v1/messages` and `/v1/messages/count_tokens`. Choose provider bases without a repeated `/v1`, for example:

```json
{
	"providers": {
		"messages": {
			"base_url": "https://api.anthropic.com",
			"auth": "passthrough"
		},
		"inference": {
			"base_url": "http://localhost:30000",
			"auth": "none",
			"catalog_file": "model-catalog.json"
		}
	},
	"routes": [
		{
			"method": "GET",
			"path": "/v1/models",
			"action": "forward",
			"provider": "messages"
		},
		{
			"path_prefix": "/v1/messages",
			"action": "model",
			"provider": "messages"
		}
	]
}
```

Save this beside a manual `model-catalog.json` catalog listing inference model IDs. The router reads each request’s native `model` field. Listed inference IDs select that provider; other IDs use the Messages fallback. This applies to both Messages and token-count requests. Restart after manual catalog edits. Provider bases must fit the complete paths sent by clients. Request bodies, thinking settings, tools, images, SSE events, and `anthropic-version`/`anthropic-beta` headers are forwarded unchanged. The upstream must implement the Messages API; the router does not translate OpenAI APIs into Messages.

## Authentication

For a saved Claude subscription login, keep your existing login and set only the base URL. Remove API-key or gateway-token overrides if you intend to use that login: `ANTHROPIC_API_KEY` and `ANTHROPIC_AUTH_TOKEN` take precedence over it. Use `/status` to confirm the base URL and active credential source.

For API billing, use `ANTHROPIC_API_KEY` with the passthrough Messages provider. Claude Code sends it as `x-api-key`:

```sh
export ANTHROPIC_BASE_URL="http://localhost:8080"
# Set ANTHROPIC_API_KEY through your normal secret-management method.
claude
```

If you instead keep credentials on the router, configure the upstream's `env_http_headers`, for example `{"x-api-key":"UPSTREAM_API_KEY"}`, and use `auth: "none"`. A client-side placeholder token may satisfy the CLI's credential check when the destination is a trusted local gateway; the router strips it before adding the upstream credential. This router does not authenticate its local listener and must remain bound to loopback.

## Persistent client settings

Merge the environment block into `~/.claude/settings.json` or a project-local `.claude/settings.local.json`:

```json
{
	"env": {
		"ANTHROPIC_BASE_URL": "http://localhost:8080",
		"ANTHROPIC_CUSTOM_MODEL_OPTION": "vendor/model",
		"ANTHROPIC_CUSTOM_MODEL_OPTION_NAME": "Custom model"
	}
}
```

The custom option adds the local ID to `/model` without replacing built-in Claude aliases. Or select it for a session with:

```sh
claude --model 'vendor/model'
```

Gateway discovery is opt-in with `CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1`. The forwarding rule above sends `/v1/models` to the configured upstream. Claude Code filters discovered IDs to ones containing `claude` or `anthropic`, so use the custom option or `modelPicker` settings for other model families.

## Subagents and context

A Claude main agent and custom-model workers can share one process and base URL:

```sh
export CLAUDE_CODE_SUBAGENT_MODEL='vendor/model'
claude
```

Register the model ID in the manual inference catalog. The router uses each request’s model ID to choose the destination; no process-wide routing header is required. Explicit spawn choices and agent-definition models may override this default. Keep quality-sensitive workers on an appropriate model.

For a full session on this unrecognized local ID, declare its actual context window if Claude Code assumes a smaller one:

```sh
export CLAUDE_CODE_MAX_CONTEXT_TOKENS=262144
claude --model 'vendor/model'
```

Do not apply a model-specific context override globally when switching between models with different limits. Declare custom supported capabilities only after verifying the local backend implements them. Anthropic does not officially support routing Claude Code to non-Claude models; compatible local usage must be validated for the features you use.

Official references: [gateway setup](https://code.claude.com/docs/en/llm-gateway-connect), [gateway protocol](https://code.claude.com/docs/en/llm-gateway-protocol), [model configuration](https://code.claude.com/docs/en/model-config), and [environment variables](https://code.claude.com/docs/en/env-vars), which documents the available model and subagent overrides. Custom headers remain optional for explicit forwarding rules; native model routing does not depend on them.
