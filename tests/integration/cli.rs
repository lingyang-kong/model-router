use std::process::{Command, Stdio};

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_model-router"));
    command
        .env(
            "MODEL_ROUTER_CONFIG",
            "/nonexistent/model-router-config.toml",
        )
        .env_remove("MODEL_ROUTER_CONFIG_TOML");
    command
}

#[test]
fn help_and_version_work_without_configuration() {
    for (flag, expected) in [
        ("--help", "MODEL_ROUTER_CONFIG"),
        ("-h", "Usage: model-router"),
        (
            "--version",
            concat!("model-router ", env!("CARGO_PKG_VERSION")),
        ),
        ("-V", concat!("model-router ", env!("CARGO_PKG_VERSION"))),
    ] {
        let output = command().arg(flag).output().unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8(output.stdout).unwrap().contains(expected));
    }
}

#[test]
fn unsupported_arguments_fail_before_loading_configuration() {
    for args in [vec!["--unknown"], vec!["--help", "extra"]] {
        let output = command().args(args).output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("Unsupported arguments")
        );
    }
}

#[test]
fn file_configuration_resolves_catalog_paths_and_overrides_inline_configuration() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("models.json"), r#"{"data":[{"id":""}]}"#).unwrap();
    let path = root.path().join("router-config.toml");
    std::fs::write(
        &path,
        r#"
[providers.api]
base_url = "http://localhost"
auth = "none"
catalog = { file = "models.json", format = "openai" }

[[routes]]
action = "model"
"#,
    )
    .unwrap();
    let output = command()
        .current_dir(std::env::temp_dir())
        .env("MODEL_ROUTER_CONFIG", path)
        .env("MODEL_ROUTER_CONFIG_TOML", "invalid inline TOML")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("catalog models need a nonempty id string")
    );
}

#[tokio::test]
async fn supplied_discovery_configuration_loads_without_a_catalog_file() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let text = std::fs::read_to_string(root.join("router-config.toml")).unwrap();
    let config = model_router::Config::from_toml(&text, &root).unwrap();
    assert!(model_router::router(config).is_ok());
    assert!(model_router::Config::from_toml(r#"{"providers":{},"routes":[]}"#, &root).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_stops_accepting_and_drains_an_inflight_request() {
    use std::{
        io::{BufRead, BufReader},
        sync::Arc,
        time::Duration,
    };
    use tokio::{
        net::TcpStream,
        sync::Notify,
        time::{sleep, timeout},
    };

    struct Process(std::process::Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let backend = crate::common::serve(axum::Router::new().fallback({
        let entered = entered.clone();
        let release = release.clone();
        move || {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                "completed"
            }
        }
    }))
    .await;
    let config = serde_json::json!({
        "providers":{"api":{"base_url":backend.url,"auth":"none"}},
        "routes":[{"action":"forward","provider":"api"}]
    });
    let mut process = Process(
        command()
            .env_remove("MODEL_ROUTER_CONFIG")
            .env(
                "MODEL_ROUTER_CONFIG_TOML",
                toml::to_string(&config).unwrap(),
            )
            .env("MODEL_ROUTER_PORT", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = process.0.stderr.take().unwrap();
    let line = timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            BufReader::new(stderr).read_line(&mut line).unwrap();
            line
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let address = line
        .trim()
        .strip_prefix("Model router listening on ")
        .unwrap()
        .to_owned();
    let port = url::Url::parse(&address).unwrap().port().unwrap();
    let pending = tokio::spawn(async move {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(address)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    });
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(process.0.id().to_string())
            .status()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(5), async {
        while TcpStream::connect(("localhost", port)).await.is_ok() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(process.0.try_wait().unwrap().is_none());
    release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap(),
        "completed"
    );
    let status = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                break status;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success());
}
