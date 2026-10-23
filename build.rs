use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=services/api/proto/scheduler.proto");
    println!("cargo:rerun-if-changed=src/image/content.proto");
    println!("cargo:rerun-if-changed=src/image/build_history.proto");
    emit_git_rerun_inputs();
    build_urma_shim();

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["services/api/proto/scheduler.proto"],
            &["services/api/proto"],
        )
        .expect("failed to compile scheduler proto for Rust gRPC client");

    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(
            &["src/image/content.proto", "src/image/build_history.proto"],
            &["src/image"],
        )
        .expect("failed to compile content store client");

    let commit = resolve_git_commit().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=AENV_GIT_COMMIT={commit}");
}

fn build_urma_shim() {
    if std::env::var_os("CARGO_FEATURE_P2P_URMA").is_none() {
        return;
    }

    println!("cargo:rerun-if-env-changed=UMDK_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=UMDK_LIB_DIR");
    println!("cargo:rerun-if-changed=src/p2p/urma/native/shim.c");
    println!("cargo:rerun-if-changed=src/p2p/urma/native/shim.h");

    let configured = std::env::var_os("UMDK_INCLUDE_DIR").map(PathBuf::from);
    let candidates = configured
        .into_iter()
        .flat_map(|path| [path.clone(), path.join("ub/umdk/urma")])
        .chain([
            PathBuf::from("/usr/include/ub/umdk/urma"),
            PathBuf::from("/usr/local/include/ub/umdk/urma"),
        ]);
    let include = candidates
        .into_iter()
        .find(|path| path.join("urma_api.h").is_file())
        .expect("p2p-urma requires urma_api.h; set UMDK_INCLUDE_DIR");

    cc::Build::new()
        .file("src/p2p/urma/native/shim.c")
        .include(include)
        .warnings(true)
        .extra_warnings(true)
        .compile("agentenv_urma_shim");

    if let Some(lib) = std::env::var_os("UMDK_LIB_DIR") {
        println!(
            "cargo:rustc-link-search=native={}",
            PathBuf::from(lib).display()
        );
    }
    println!("cargo:rustc-link-lib=dylib=urma");
    println!("cargo:rustc-link-lib=dylib=dl");
}

fn emit_git_rerun_inputs() {
    if let Some(path) = git_path("HEAD") {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let Some(head_ref) = git_stdout(["symbolic-ref", "-q", "HEAD"]) else {
        return;
    };
    let head_ref = head_ref.trim();
    if head_ref.is_empty() {
        return;
    }

    if let Some(path) = git_path(head_ref) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    // Track packed refs as well as the loose branch ref so changes after
    // repacking or worktree-local HEAD updates still refresh the embedded SHA.
    if let Some(path) = git_path("packed-refs").filter(|path| path.exists()) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_path(path: &str) -> Option<PathBuf> {
    git_stdout(["rev-parse", "--git-path", path]).map(|path| PathBuf::from(path.trim()))
}

fn resolve_git_commit() -> Option<String> {
    let commit = git_stdout(["rev-parse", "--short", "HEAD"])?;
    let commit = commit.trim();
    (!commit.is_empty()).then(|| commit.to_string())
}

fn git_stdout<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout).ok()
}
