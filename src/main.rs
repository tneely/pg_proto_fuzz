mod comparator;
mod connection;
mod generator;
#[cfg(test)]
mod mock;
mod op;
mod profile;
mod runner;
mod shrinker;
mod template;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use connection::TcpConnectionFactory;
use generator::Generator;
use profile::FuzzProfile;
use runner::DualRunner;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(name = "pg_proto_fuzz", about = "Postgres wire protocol fuzzer")]
struct Cli {
    /// Oracle Postgres host
    #[arg(long, default_value = "localhost")]
    pg_host: String,

    /// Oracle Postgres port
    #[arg(long, default_value_t = 5432)]
    pg_port: u16,

    /// Oracle Postgres user
    #[arg(long, default_value = "postgres")]
    pg_user: String,

    /// Oracle Postgres password
    #[arg(long)]
    pg_password: Option<String>,

    /// Oracle Postgres database
    #[arg(long, default_value = "postgres")]
    pg_database: String,

    /// Target host [default: same as --pg-host]
    #[arg(long)]
    target_host: Option<String>,

    /// Target port [default: same as --pg-port]
    #[arg(long)]
    target_port: Option<u16>,

    /// Target user [default: same as --pg-user]
    #[arg(long)]
    target_user: Option<String>,

    /// Target password [default: same as --pg-password]
    #[arg(long)]
    target_password: Option<String>,

    /// Target database [default: same as --pg-database]
    #[arg(long)]
    target_database: Option<String>,

    /// Number of fuzz iterations
    #[arg(short = 'n', long, default_value_t = 1000)]
    iterations: usize,

    /// RNG seed for reproducibility [default: random]
    #[arg(long)]
    seed: Option<u64>,

    /// Feature profile: minimal, standard, full
    #[arg(long, default_value = "minimal")]
    profile: String,

    /// Enable feature tags (comma-separated)
    #[arg(long, value_delimiter = ',')]
    enable: Vec<String>,

    /// Disable feature tags (comma-separated)
    #[arg(long, value_delimiter = ',')]
    disable: Vec<String>,

    /// Per-response-collection timeout in milliseconds
    #[arg(long, default_value_t = 2000)]
    timeout: u64,

    /// Number of parallel workers
    #[arg(long, default_value_t = 10)]
    workers: usize,
}

fn parse_profile(name: &str) -> FuzzProfile {
    match name {
        "minimal" => FuzzProfile::minimal(),
        "standard" => FuzzProfile::standard(),
        "full" => FuzzProfile::full(),
        other => {
            tracing::warn!("Unknown profile: {other}. Using 'minimal'.");
            FuzzProfile::minimal()
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let seed = cli.seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });

    let target_host = cli.target_host.unwrap_or_else(|| cli.pg_host.clone());
    let target_port = cli.target_port.unwrap_or(cli.pg_port);
    let target_user = cli.target_user.unwrap_or_else(|| cli.pg_user.clone());
    let target_password = cli.target_password.or_else(|| cli.pg_password.clone());
    let target_database = cli
        .target_database
        .unwrap_or_else(|| cli.pg_database.clone());

    let mut profile = parse_profile(&cli.profile);
    for tag in &cli.enable {
        let tag: &'static str = Box::leak(tag.clone().into_boxed_str());
        profile.enable(tag);
    }
    for tag in &cli.disable {
        profile.disable(tag);
    }

    let timeout = Duration::from_millis(cli.timeout);

    tracing::info!(
        oracle = %format_args!("{}@{}:{}/{}", cli.pg_user, cli.pg_host, cli.pg_port, cli.pg_database),
        target = %format_args!("{target_user}@{target_host}:{target_port}/{target_database}"),
        iterations = cli.iterations,
        seed,
        profile = %cli.profile,
        timeout_ms = cli.timeout,
        workers = cli.workers,
        "pg_proto_fuzz starting",
    );

    let pg_factory = TcpConnectionFactory {
        host: cli.pg_host,
        port: cli.pg_port,
        user: cli.pg_user,
        password: cli.pg_password,
        database: cli.pg_database,
    };
    let target_factory = TcpConnectionFactory {
        host: target_host,
        port: target_port,
        user: target_user,
        password: target_password,
        database: target_database,
    };

    let setup_stmts: Vec<String> = template::setup_sql(&profile)
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    let dual_runner =
        Arc::new(DualRunner::new(pg_factory, target_factory, timeout).with_setup(setup_stmts));

    let mut generator = Generator::new(&profile, seed);
    let divergence_count = Arc::new(AtomicUsize::new(0));
    let completed_count = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(cli.workers));
    let start = Instant::now();
    let total = cli.iterations;

    let mut join_set = JoinSet::new();

    for i in 0..total {
        // Generate ops sequentially to preserve seed reproducibility
        let ops = generator.next();

        // Acquire permit before spawning (backpressure when all workers busy)
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let runner = dual_runner.clone();
        let div_count = divergence_count.clone();
        let done_count = completed_count.clone();

        join_set.spawn(async move {
            let _permit = permit;

            match runner.run(&ops).await {
                Ok((pg_resp, target_resp)) => {
                    if let Some(div) = comparator::compare(&pg_resp, &target_resp, &ops) {
                        let count = div_count.fetch_add(1, Ordering::Relaxed) + 1;
                        let original_len = ops.len();
                        let shrunk_ops = shrinker::shrink(&ops, &div, &runner).await;

                        // Re-run the shrunk sequence to get a clean divergence report
                        if let Ok((pg2, t2)) = runner.run(&shrunk_ops).await
                            && let Some(shrunk_div) = comparator::compare(&pg2, &t2, &shrunk_ops)
                        {
                            tracing::warn!(
                                count,
                                original_ops = original_len,
                                shrunk_ops = shrunk_ops.len(),
                                "DIVERGENCE (shrunk {original_len} -> {} ops)\n{shrunk_div}",
                                shrunk_ops.len(),
                            );
                        } else {
                            // Shrunk sequence didn't reproduce — report original
                            tracing::warn!(count, "DIVERGENCE\n{div}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(iteration = i, "connection error: {e}");
                }
            }

            let done = done_count.fetch_add(1, Ordering::Relaxed) + 1;
            if done.is_multiple_of(100) || done == total {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = done as f64 / elapsed;
                let divs = div_count.load(Ordering::Relaxed);
                tracing::info!(
                    progress = %format_args!("{done}/{total}"),
                    rate = %format_args!("{rate:.0}"),
                    divergences = divs,
                    "progress",
                );
            }
        });
    }

    // Wait for all workers to finish
    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            tracing::error!("worker panic: {e}");
        }
    }

    let divergence_total = divergence_count.load(Ordering::Relaxed);
    tracing::info!(
        iterations = total,
        divergences = divergence_total,
        seed,
        "done",
    );

    if divergence_total > 0 {
        std::process::exit(1);
    }
}
