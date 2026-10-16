use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const CODEX_REVISION: &str = "092d3acd6bec3e3a14bdc7e7a2810ab628ab759d";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    let prompt_dir = out_dir.join(format!("codex-prompts-{CODEX_REVISION}"));
    fs::create_dir_all(&prompt_dir).expect("create Codex prompt output directory");
    println!("cargo:rustc-env=CODEX_PROMPTS_DIR={}", prompt_dir.display());

    fetch_if_missing(
        &prompt_dir,
        "compact_prompt.txt",
        "codex-rs/prompts/templates/compact/prompt.md",
    );
    fetch_if_missing(
        &prompt_dir,
        "summary_prefix.txt",
        "codex-rs/prompts/templates/compact/summary_prefix.md",
    );
    fetch_if_missing(&prompt_dir, "CODEX_PROMPTS_LICENSE.txt", "LICENSE");
}

fn fetch_if_missing(output_dir: &Path, filename: &str, source_path: &str) {
    let output = output_dir.join(filename);
    if valid_text(&output) {
        return;
    }

    let url =
        format!("https://raw.githubusercontent.com/openai/codex/{CODEX_REVISION}/{source_path}");
    let temporary = output_dir.join(format!(".{filename}.{}.tmp", std::process::id()));
    let status = Command::new("curl")
        .args([
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--output",
        ])
        .arg(&temporary)
        .arg(&url)
        .status()
        .unwrap_or_else(|error| panic!("run curl to fetch {url}: {error}"));
    if !status.success() {
        let _ = fs::remove_file(&temporary);
        panic!("fetch {url} failed with {status}");
    }
    if !valid_text(&temporary) {
        let _ = fs::remove_file(&temporary);
        panic!("fetched {url} is empty or not valid UTF-8 text");
    }
    fs::rename(&temporary, &output).unwrap_or_else(|error| {
        let _ = fs::remove_file(&temporary);
        panic!("install fetched file {}: {error}", output.display());
    });
}

fn valid_text(path: &Path) -> bool {
    fs::read_to_string(path).is_ok_and(|text| !text.trim().is_empty())
}
