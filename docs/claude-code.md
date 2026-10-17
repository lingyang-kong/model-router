# Claude Code

Upstreams must support the Anthropic Messages API, including token counting. Non-Claude models need compatibility testing.

## Router setup

Adapt this example for Anthropic and a Messages-compatible inference server, then [start the router](../README.md#run):

```toml
[providers.anthropic]
base_url = "https://api.anthropic.com"
auth = "passthrough"

[providers.inference]
base_url = "http://localhost:30000"
auth = "none"
catalog = { file = "model-catalog.json", format = "anthropic" }

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

Save `model-catalog.json` beside it:

```json
{"data":[{"id":"vendor/model"}]}
```

Replace the inference URL and `vendor/model` with your server's URL and exact model ID. Listed IDs route to inference; other IDs use Anthropic. Claude sends `/v1/messages` and `/v1/messages/count_tokens`, so these base URLs omit `/v1`. Restart after catalog edits.

## Client setup

```sh
export ANTHROPIC_BASE_URL="http://localhost:4134"
export ANTHROPIC_CUSTOM_MODEL_OPTION="vendor/model"
export ANTHROPIC_CUSTOM_MODEL_OPTION_NAME="Custom model"
claude
```

The custom option adds the model to `/model`. To select it directly, run `claude --model 'vendor/model'`. Persist these variables in the `env` object of `~/.claude/settings.json` or `.claude/settings.local.json`.

For custom workers alongside a Claude main agent:

```sh
export CLAUDE_CODE_SUBAGENT_MODEL="vendor/model"
claude
```

Explicit agent model choices can override this default. Omit `x-router-client`.

## Authentication

- **Subscription:** keep your login and leave `ANTHROPIC_API_KEY` and `ANTHROPIC_AUTH_TOKEN` unset. Check the active credential source with `/status`.
- **API key:** set `ANTHROPIC_API_KEY`; the passthrough provider forwards it.
- **Router-held key:** use `auth = "none"` and `env_http_headers = { "x-api-key" = "UPSTREAM_API_KEY" }` on the provider. The client may still require a placeholder credential. The router's listener is unauthenticated and binds to loopback.

## Optional discovery and context

`CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1` enables discovery through the `/v1/models` forwarding rule. Claude filters discovered IDs to those containing `claude` or `anthropic`; use the custom model option for other families.

For a full session on an unrecognized model, `CLAUDE_CODE_MAX_CONTEXT_TOKENS` can declare its actual context window. Avoid a global override when switching between models with different limits.

References: [gateway protocol](https://code.claude.com/docs/en/llm-gateway-protocol), [model configuration](https://code.claude.com/docs/en/model-config), [environment variables](https://code.claude.com/docs/en/env-vars).
