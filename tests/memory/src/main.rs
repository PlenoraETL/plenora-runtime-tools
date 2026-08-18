//! Command-line entry point for the bounded Linux RSS memory probe.

#![forbid(unsafe_code)]

use std::{env, process::ExitCode, time::Duration};

use plenora_runtime_memory_tests::{
    MemoryScenario, ProbeConfig, ProbeReport, RetentionClassification, run_probe,
};

const MIB: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    match config_from_environment() {
        Ok(config) => match run_probe(config).await {
            Ok(report) => {
                print_csv(&report);
                if report.summary.classification == RetentionClassification::Stable {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(2)
                }
            }
            Err(error) => {
                eprintln!("memory probe failed: {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("memory probe configuration failed: {error}");
            ExitCode::from(1)
        }
    }
}

fn config_from_environment() -> Result<ProbeConfig, Box<dyn std::error::Error>> {
    let scenario_text = env::var("PLENORA_MEMORY_SCENARIO")?;
    let scenario = MemoryScenario::try_from(scenario_text.as_str())?;
    let defaults = ProbeConfig::for_scenario(scenario);
    let allocation_mib =
        optional_usize("PLENORA_MEMORY_ALLOCATION_MIB")?.unwrap_or(defaults.allocation_bytes / MIB);
    let retained_growth_limit_mib = optional_usize("PLENORA_MEMORY_GROWTH_LIMIT_MIB")?
        .unwrap_or(defaults.retained_growth_limit_bytes / MIB);
    let allocation_bytes = allocation_mib
        .checked_mul(MIB)
        .ok_or("memory allocation byte count overflowed")?;
    let retained_growth_limit_bytes = retained_growth_limit_mib
        .checked_mul(MIB)
        .ok_or("memory growth limit byte count overflowed")?;
    Ok(ProbeConfig {
        scenario,
        iterations: optional_usize("PLENORA_MEMORY_ITERATIONS")?.unwrap_or(defaults.iterations),
        warmup_iterations: optional_usize("PLENORA_MEMORY_WARMUP_ITERATIONS")?
            .unwrap_or(defaults.warmup_iterations),
        allocation_bytes,
        max_in_flight: optional_usize("PLENORA_MEMORY_MAX_IN_FLIGHT")?
            .unwrap_or(defaults.max_in_flight),
        settle_period: Duration::from_millis(
            optional_u64("PLENORA_MEMORY_SETTLE_MILLIS")?
                .unwrap_or(defaults.settle_period.as_millis().try_into()?),
        ),
        retained_growth_limit_bytes,
    })
}

fn optional_usize(name: &str) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(Into::into)
}

fn optional_u64(name: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(Into::into)
}

fn print_csv(report: &ProbeReport) {
    println!(
        "record_type,scenario,iteration,rss_bytes,peak_rss_bytes,live_bytes,peak_live_bytes,worker_in_flight,outcome,initial_median_rss_bytes,final_median_rss_bytes,retained_growth_bytes,growth_limit_bytes,classification"
    );
    for sample in &report.samples {
        println!(
            "sample,{},{},{},{},{},{},{},{},,,,,",
            report.config.scenario,
            sample.iteration,
            sample.process.rss_bytes,
            sample.process.peak_rss_bytes,
            sample.allocations.live_bytes,
            sample.allocations.peak_live_bytes,
            sample.worker_in_flight,
            sample.outcome,
        );
    }
    println!(
        "summary,{},,,,,,,,{},{},{},{},{}",
        report.config.scenario,
        report.summary.initial_median_rss_bytes,
        report.summary.final_median_rss_bytes,
        report.summary.retained_growth_bytes,
        report.summary.retained_growth_limit_bytes,
        report.summary.classification.as_str(),
    );
}
