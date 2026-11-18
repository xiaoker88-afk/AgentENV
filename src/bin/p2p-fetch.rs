//! Standalone P2P artifact smoke-test and micro-benchmark tool.
//!
//! All operations go through the same `P2pTransport` abstraction used by the
//! AgentENV server. The benchmark consumes a byte-range stream without writing
//! it to disk, so destination storage does not dominate transport throughput.

use std::path::{Path, PathBuf};
use std::time::Instant;

use agentenv::identity::NodeIdentity;
use agentenv::p2p::{
    transport_from_config, P2pArtifactProvider, P2pArtifactProviderHint, P2pEndpoint,
    P2pPublishRequest, P2pTransport,
};
use anyhow::{bail, Context};
use futures::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug)]
enum Command {
    Serve {
        file: PathBuf,
        key: String,
    },
    Fetch {
        key: String,
        destination: PathBuf,
    },
    Benchmark {
        key: String,
        peer: String,
        offset: u64,
        bytes: usize,
        expected_sha256: Option<String>,
        result: PathBuf,
    },
}

#[derive(Serialize)]
struct BenchmarkResult {
    schema: &'static str,
    timestamp: String,
    transport: String,
    local_endpoint: String,
    peer_endpoint: String,
    key: String,
    offset: u64,
    requested_bytes: usize,
    received_bytes: usize,
    transport_init_ms: f64,
    lookup_ms: f64,
    stream_prepare_ms: f64,
    first_byte_ms: f64,
    transfer_ms: f64,
    throughput_mib_s: f64,
    sha256: Option<String>,
    sha256_verified: Option<bool>,
}

fn usage() -> &'static str {
    "usage:\n  p2p-fetch --serve <file> <key>\n  p2p-fetch <key> [destination]\n  p2p-fetch --benchmark <key> --peer <endpoint> --bytes <count> [--offset <count>] [--sha256 <hex>] [--result <path>]"
}

fn parse_u64(name: &str, raw: &str) -> anyhow::Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("invalid {name} value '{raw}'"))
}

fn option_value(args: &[String], name: &str) -> anyhow::Result<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    let value = args
        .get(index + 1)
        .with_context(|| format!("missing value after {name}"))?;
    Ok(Some(value.clone()))
}

fn parse_command(args: &[String]) -> anyhow::Result<Command> {
    if args.first().map(String::as_str) == Some("--serve") {
        if args.len() != 3 {
            bail!(usage());
        }
        return Ok(Command::Serve {
            file: PathBuf::from(&args[1]),
            key: args[2].clone(),
        });
    }

    if args.first().map(String::as_str) == Some("--benchmark") {
        let key = args.get(1).cloned().context(usage())?;
        let peer = option_value(args, "--peer")?.context("--benchmark requires --peer")?;
        let bytes_u64 = parse_u64(
            "--bytes",
            &option_value(args, "--bytes")?.context("--benchmark requires --bytes")?,
        )?;
        let bytes = usize::try_from(bytes_u64).context("--bytes exceeds this platform's usize")?;
        if bytes == 0 {
            bail!("--bytes must be greater than zero");
        }
        let offset = option_value(args, "--offset")?
            .map(|raw| parse_u64("--offset", &raw))
            .transpose()?
            .unwrap_or(0);
        let expected_sha256 = option_value(args, "--sha256")?;
        let result = option_value(args, "--result")?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("p2p-fetch-result.json"));
        return Ok(Command::Benchmark {
            key,
            peer,
            offset,
            bytes,
            expected_sha256,
            result,
        });
    }

    let key = args.first().cloned().context(usage())?;
    if args.len() > 2 {
        bail!(usage());
    }
    Ok(Command::Fetch {
        key,
        destination: args
            .get(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/p2p-fetch.out")),
    })
}

async fn lookup(
    transport: &dyn P2pTransport,
    key: &str,
    peer: Option<P2pEndpoint>,
) -> anyhow::Result<agentenv::p2p::P2pArtifactDescriptor> {
    let hints = peer
        .map(|endpoint| {
            vec![P2pArtifactProviderHint {
                node_id: None,
                endpoint: Some(endpoint),
            }]
        })
        .unwrap_or_default();
    transport
        .lookup_with_hints(&key.to_string(), &hints)
        .await?
        .with_context(|| format!("artifact not found: {key}"))
}

fn print_descriptor(descriptor: &agentenv::p2p::P2pArtifactDescriptor) {
    println!("[lookup] backend_locator: {:?}", descriptor.backend_locator);
    println!(
        "[lookup] providers: {:?}",
        descriptor
            .providers
            .iter()
            .map(|provider| match provider {
                P2pArtifactProvider::Peer(peer) => {
                    format!("peer {} {}", peer.node_id, peer.endpoint.address)
                }
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
    );
}

async fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let json = serde_json::to_vec_pretty(value)?;
    tokio::fs::write(path, json)
        .await
        .with_context(|| format!("write benchmark result {}", path.display()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,agentenv::p2p=debug".into()),
        )
        .init();

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let command = parse_command(&args)?;
    let init_started = Instant::now();
    let config_manager = agentenv::cfg::ConfigManager::init_global()?;
    let config = config_manager.config();
    let identity = NodeIdentity::from_config(&config.node_identity);
    let transport = transport_from_config(config, &identity).await?;
    let transport_init_ms = init_started.elapsed().as_secs_f64() * 1000.0;
    let local_endpoint = transport
        .local_endpoint()
        .context("configured P2P transport has no local endpoint")?;
    println!("[init] transport: {}", local_endpoint.backend);
    println!("[init] duration: {transport_init_ms:.3} ms");
    println!("[init] local endpoint: {}", local_endpoint.address);

    match command {
        Command::Serve { file, key } => {
            let file_size = tokio::fs::metadata(&file).await?.len();
            let publish_started = Instant::now();
            transport
                .publish(&P2pPublishRequest::file(key.clone(), file.clone()))
                .await?;
            println!(
                "[serve] published {} bytes in {:.3} ms",
                file_size,
                publish_started.elapsed().as_secs_f64() * 1000.0
            );
            println!("[serve] file: {}", file.display());
            println!("[serve] key: {key}");
            println!("[serve] endpoint: {}", local_endpoint.address);
            println!("[serve] serving until interrupted...");
            tokio::signal::ctrl_c().await?;
        }
        Command::Fetch { key, destination } => {
            let lookup_started = Instant::now();
            let descriptor = lookup(transport.as_ref(), &key, None).await?;
            println!(
                "[lookup] completed in {:.3} ms",
                lookup_started.elapsed().as_secs_f64() * 1000.0
            );
            print_descriptor(&descriptor);
            let fetch_started = Instant::now();
            let size = transport.fetch(&descriptor, &destination).await?;
            let elapsed = fetch_started.elapsed();
            println!(
                "[fetch] received {size} bytes in {:.3} ms ({:.2} MiB/s) -> {}",
                elapsed.as_secs_f64() * 1000.0,
                size as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64().max(1e-9),
                destination.display()
            );
        }
        Command::Benchmark {
            key,
            peer,
            offset,
            bytes,
            expected_sha256,
            result,
        } => {
            let peer_endpoint = P2pEndpoint {
                backend: local_endpoint.backend.clone(),
                address: peer.clone(),
            };
            let lookup_started = Instant::now();
            let descriptor = lookup(transport.as_ref(), &key, Some(peer_endpoint)).await?;
            let lookup_ms = lookup_started.elapsed().as_secs_f64() * 1000.0;
            print_descriptor(&descriptor);

            let transfer_started = Instant::now();
            let prepare_started = Instant::now();
            let mut stream = transport
                .fetch_byte_range(&descriptor, offset, bytes)
                .await?;
            let stream_prepare_ms = prepare_started.elapsed().as_secs_f64() * 1000.0;
            let mut first_byte_ms = None;
            let mut received = 0usize;
            // Hashing is optional because SHA-256 can become the bottleneck on
            // fast URMA links. Use it for correctness runs, not throughput runs.
            let mut hasher = expected_sha256.as_ref().map(|_| Sha256::new());
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                first_byte_ms
                    .get_or_insert_with(|| transfer_started.elapsed().as_secs_f64() * 1000.0);
                received = received
                    .checked_add(chunk.len())
                    .context("received byte count overflow")?;
                if let Some(hasher) = hasher.as_mut() {
                    hasher.update(&chunk);
                }
            }
            let transfer_ms = transfer_started.elapsed().as_secs_f64() * 1000.0;
            if received != bytes {
                bail!("short benchmark read: requested {bytes} bytes, received {received}");
            }
            let digest = hasher.map(|hasher| hex::encode(hasher.finalize()));
            let verified = expected_sha256
                .as_ref()
                .zip(digest.as_ref())
                .map(|(expected, digest)| expected.eq_ignore_ascii_case(digest));
            if verified == Some(false) {
                bail!(
                    "SHA-256 mismatch: expected {}, received {}",
                    expected_sha256.as_deref().unwrap_or_default(),
                    digest.as_deref().unwrap_or_default()
                );
            }
            let throughput_mib_s =
                received as f64 / 1024.0 / 1024.0 / (transfer_ms / 1000.0).max(1e-9);
            let report = BenchmarkResult {
                schema: "agentenv.p2p-fetch-benchmark.v1",
                timestamp: chrono::Utc::now().to_rfc3339(),
                transport: local_endpoint.backend,
                local_endpoint: local_endpoint.address,
                peer_endpoint: peer,
                key,
                offset,
                requested_bytes: bytes,
                received_bytes: received,
                transport_init_ms,
                lookup_ms,
                stream_prepare_ms,
                first_byte_ms: first_byte_ms.context("benchmark stream returned no data")?,
                transfer_ms,
                throughput_mib_s,
                sha256: digest,
                sha256_verified: verified,
            };
            write_json(&result, &report).await?;
            println!(
                "[benchmark] received {} bytes in {:.3} ms ({:.2} MiB/s)",
                report.received_bytes, report.transfer_ms, report.throughput_mib_s
            );
            if let Some(digest) = &report.sha256 {
                println!("[benchmark] SHA-256: {digest}");
            }
            println!("[benchmark] result: {}", result.display());
        }
    }

    transport.shutdown().await?;
    Ok(())
}
