# Connect Codex

Use GPT through your ChatGPT subscription and custom models through any Responses-compatible upstream. Both appear in Codex's model picker.

## 1. Start the router

Set the upstream URL in the [router configuration](../examples/router-config.json) and your model's exact ID and capabilities in the [Codex model catalog](../examples/codex-model-catalog.json). The supplied files use the [Qwen over SGLang example](#example-qwen-over-sglang) below. From the repository directory:

```sh
MODEL_ROUTER_CONFIG=examples/router-config.json cargo run --release --locked
```

The router listens on `localhost:8080`. For an existing [systemd service](../README.md#systemd), set `MODEL_ROUTER_CONFIG` to the absolute path of `examples/router-config.json` and restart it.

## 2. Configure Codex

Set `model_provider` before the first table in `~/.codex/config.toml`, then merge the provider table:

```toml
model_provider = "router"

[model_providers.router]
name = "OpenAI" # for HTTP header key x-codex-routing-hint
base_url = "http://localhost:8080"
model_catalog_url = "http://localhost:8080/models"
requires_openai_auth = true
supports_websockets = true
```

Both provider flags default to `false` for custom providers. Keep your ChatGPT login, or run `codex login`. This setup needs no OpenAI API key. WebSockets require support from every selected upstream. Set `supports_websockets = false` if any upstream supports only HTTP Responses.

## 3. Start Codex

```sh
codex
```

Open `/model` and select a model. Requests use that model's registered upstream.

## Worker models

To select a default worker model, merge this into `~/.codex/config.toml`, replacing `<model-id>` with its exact catalog ID:

```toml
[agents]
default_subagent_model = "<model-id>"
```

Workers inherit the router provider; explicit model choices and role settings can override this default. Set a reasoning effort only if that model supports it. Restart the router and Codex after manual catalog edits. GPT's catalog refreshes from the subscription service.

## Example: Qwen over SGLang

The supplied configuration routes GPT to the ChatGPT subscription service and `nvidia/Qwen3.8-Flash-Next-NVFP4` to SGLang at `http://localhost:30000/v1`. Use that model ID in the worker setting above to run Qwen workers alongside a GPT main agent. If the SGLang endpoint supports only HTTP Responses, set `supports_websockets = false` in the Codex provider table.

Qwen's catalog entry follows `gpt-6-luna` from Codex 0.162.1's bundled catalog, including code mode, client-side utility tools, and multi-agent v2. Its base instructions have model-identity and grammar corrections. The reference comes from `codex debug models --bundled`.

The tested Qwen deployment supports `none`, `low`, `medium`, and `xhigh`, with `xhigh` as its default and a 262,144-token context limit. Its SGLang backend does not implement verbosity control, original image detail, native tool search, or Responses Lite, so those GPT settings are not enabled in this example. Adapt the catalog to your own model and serving backend.

## WebSocket routing

With WebSockets, Codex 0.162 uses `name = "OpenAI"` and ChatGPT sign-in to supply the model routing hint. Reconnect when changing providers; the destination is fixed for each connection.

[Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
