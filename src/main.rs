use model_router::{Config, router};
use tokio::net::TcpListener;

const PORT_ENV: &str = "MODEL_ROUTER_PORT";
const DEFAULT_PORT: &str = "4134";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let check_config = match (args.next(), args.next()) {
        (None, None) => false,
        (Some(arg), None) if arg == "--help" || arg == "-h" => {
            println!(
                "Usage: model-router [--help | --version | --check-config]\n\n\
                 --check-config\tValidate startup configuration and print resolved overrides as JSON\n\n\
                 Environment:\n\t\
                 MODEL_ROUTER_CONFIG\t\tPath to the router TOML configuration\n\t\
                 MODEL_ROUTER_CONFIG_TOML\tInline TOML when no configuration path is set\n\t\
                 MODEL_ROUTER_PORT\t\tLocalhost port (default: {DEFAULT_PORT})"
            );
            return Ok(());
        }
        (Some(arg), None) if arg == "--version" || arg == "-V" => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        (Some(arg), None) if arg == "--check-config" => true,
        _ => anyhow::bail!("Unsupported arguments; run model-router --help"),
    };
    let config = Config::from_env()?;
    if check_config {
        let report =
            serde_json::json!({"resolved_model_overrides": config.resolved_model_overrides()});
        let _ = router(config)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    let port = std::env::var(PORT_ENV)
        .unwrap_or_else(|_| DEFAULT_PORT.into())
        .parse::<u16>()?;
    let app = router(config)?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let listener = TcpListener::bind(("localhost", port)).await?;
    eprintln!(
        "Model router listening on http://localhost:{}/",
        listener.local_addr()?.port()
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
