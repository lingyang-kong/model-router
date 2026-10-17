use model_router::{Config, router};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    match (args.next(), args.next()) {
        (None, None) => {}
        (Some(arg), None) if arg == "--help" || arg == "-h" => {
            println!(
                "Usage: model-router [--help | --version]\n\n\
                 Environment:\n\t\
                 MODEL_ROUTER_CONFIG\t\tPath to the router TOML configuration\n\t\
                 MODEL_ROUTER_CONFIG_TOML\tInline TOML when no configuration path is set\n\t\
                 MODEL_ROUTER_PORT\t\tLocalhost port (default: 4134)"
            );
            return Ok(());
        }
        (Some(arg), None) if arg == "--version" || arg == "-V" => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        _ => anyhow::bail!("Unsupported arguments; run model-router --help"),
    }
    let config = Config::from_env()?;
    let port = std::env::var("MODEL_ROUTER_PORT")
        .unwrap_or_else(|_| "4134".into())
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
