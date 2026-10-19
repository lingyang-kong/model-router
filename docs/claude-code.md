# Claude Code

Upstreams must support the Anthropic Messages API. Token counting is optional; without it Claude Code estimates context usage. Non-Claude models need compatibility testing.

## Router setup

Configure Anthropic and a Messages-compatible inference server, then [start the router](../README.md#run):

```toml
[providers.anthropic]
base_url = "https://api.anthropic.com"
auth = "passthrough"

[providers.inference]
base_url = "http://localhost:30000"
auth = "none"

[providers.inference.models."vendor/model"]

[[routes]]
method = "GET"
path = "/v1/models"
action = "forward"
provider = "anthropic"

[[routes]]
path_prefix = "/v1/messages"
action = "model"
provider = "anthropic"
```

Replace the inference URL and `vendor/model` with your server's URL and exact model ID. The explicit model table registers its route at startup; other IDs use Anthropic. These base URLs omit `/v1` because Claude supplies `/v1/messages` and `/v1/messages/count_tokens`.

## Client setup

```sh
export ANTHROPIC_BASE_URL="http://localhost:4134"
export ANTHROPIC_CUSTOM_MODEL_OPTION="vendor/model"
export ANTHROPIC_CUSTOM_MODEL_OPTION_NAME="Custom model"
claude
```

Select the custom option with `/model`, or run `claude --model 'vendor/model'`. Persist variables in the `env` object of `~/.claude/settings.json` or `.claude/settings.local.json`.

Set `CLAUDE_CODE_SUBAGENT_MODEL="vendor/model"` for custom workers alongside a Claude main agent. Explicit agent model choices can override that default.

## Authentication and optional settings

- **Subscription:** keep your login and leave `ANTHROPIC_API_KEY` and `ANTHROPIC_AUTH_TOKEN` unset. Check the active credential source with `/status`.
- **API key:** set `ANTHROPIC_API_KEY`; the passthrough provider forwards it.
- **Router-held key:** use `auth = "none"` and `env_http_headers = { "x-api-key" = "UPSTREAM_API_KEY" }`. The client may still need a placeholder credential. See the router's [access boundary](../README.md#run).

`CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1` enables discovery through the `/v1/models` forwarding rule above. Claude filters discovered IDs to those containing `claude` or `anthropic`; use the custom model option for other families.

`CLAUDE_CODE_MAX_CONTEXT_TOKENS` can declare an unrecognized model's actual context window. Avoid a global override when switching between models with different limits.

References: [gateway protocol](https://code.claude.com/docs/en/llm-gateway-protocol), [model configuration](https://code.claude.com/docs/en/model-config), [environment variables](https://code.claude.com/docs/en/env-vars).
