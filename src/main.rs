use std::{
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::future::join_all;
use reqwest::{header, Client};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct Feed {
    name: &'static str,
    url: String,
    redis_key: &'static str,
}

#[derive(Serialize)]
struct Snapshot {
    fetched_at_unix_ms: u128,
    content_type: String,
    body_base64: String,
}

struct Config {
    api_key: String,
    redis_url: String,
    redis_token: String,
    poll_interval: Duration,
    fetch_timeout: Duration,
    redis_timeout: Duration,
    snapshot_ttl_secs: u64,
}

fn required_env(name: &str) -> AppResult<String> {
    env::var(name).map_err(|_| {
        format!("Required environment variable {name} is missing").into()
    })
}

fn positive_u64(name: &str, default: u64) -> AppResult<u64> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<u64>()?,
        Err(env::VarError::NotPresent) => default,
        Err(err) => return Err(err.into()),
    };

    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }

    Ok(value)
}

fn unix_time_ms() -> AppResult<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis())
}

fn load_config() -> AppResult<Config> {
    Ok(Config {
        api_key: required_env("GTFS_API_KEY")?,
        redis_url: required_env("UPSTASH_REDIS_REST_URL")?
            .trim_end_matches('/')
            .to_owned(),
        redis_token: required_env("UPSTASH_REDIS_REST_TOKEN")?,
        poll_interval: Duration::from_secs(positive_u64(
            "POLL_INTERVAL_SECS", 30,
        )?),
        fetch_timeout: Duration::from_secs(positive_u64(
            "FETCH_TIMEOUT_SECS", 8,
        )?),
        redis_timeout: Duration::from_secs(positive_u64(
            "REDIS_TIMEOUT_SECS", 5,
        )?),
        snapshot_ttl_secs: positive_u64("SNAPSHOT_TTL_SECS", 120)?,
    })
}

fn load_feeds() -> AppResult<Vec<Feed>> {
    Ok(vec![
        Feed {
            name: "vehicle_positions",
            url: required_env("GTFS_TRAM_VEHICLE_POSITIONS_URL")?,
            redis_key: "gtfs:realtime:tram:vehicle_positions",
        },
        Feed {
            name: "trip_updates",
            url: required_env("GTFS_TRAM_TRIP_UPDATES_URL")?,
            redis_key: "gtfs:realtime:tram:trip_updates",
        },
        Feed {
            name: "service_alerts",
            url: required_env("GTFS_TRAM_SERVICE_ALERTS_URL")?,
            redis_key: "gtfs:realtime:tram:service_alerts",
        },
    ])
}

async fn fetch_and_store(
    client: &Client,
    config: &Config,
    feed: &Feed,
) -> AppResult<()> {
    // Download the original protobuf bytes without decoding them.
    let response = client
        .get(&feed.url)
        .header("KeyID", &config.api_key)
        .header(header::ACCEPT, "application/x-protobuf")
        .send()
        .await?
        .error_for_status()?;

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/x-protobuf")
        .to_owned();

    let body = response.bytes().await?;

    if body.is_empty() {
        return Err(format!("{} returned an empty body", feed.name).into());
    }

    let snapshot = Snapshot {
        fetched_at_unix_ms: unix_time_ms()?,
        content_type,
        body_base64: STANDARD.encode(&body),
    };

    let value = serde_json::to_string(&snapshot)?;

    // One SET command atomically replaces the complete snapshot.
    // The EX argument sets the snapshot expiry in seconds.
    let redis_command = json!([
        "SET",
        feed.redis_key,
        value,
        "EX",
        config.snapshot_ttl_secs
    ]);

    let redis_response = client
        .post(format!("{}/", config.redis_url))
        .bearer_auth(&config.redis_token)
        .json(&redis_command)
        .send()
        .await?
        .error_for_status()?;

    let redis_result: Value = redis_response.json().await?;

    if let Some(redis_error) = redis_result.get("error") {
        return Err(format!(
            "Upstash rejected the SET for {}: {}",
            feed.name, redis_error
        )
            .into());
    }

    info!(
        feed = feed.name,
        bytes = body.len(),
        key = feed.redis_key,
        "Feed fetched and cached"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    // Loads .env when present; deployed environment variables take precedence.
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = load_config()?;
    let feeds = load_feeds()?;

    let fetch_client = Client::builder()
        .timeout(config.fetch_timeout)
        .build()?;

    let redis_client = Client::builder()
        .timeout(config.redis_timeout)
        .build()?;

    // Fail fast if credentials, connectivity, or Redis configuration
    // are invalid, rather than waiting for the first polling cycle.
    let redis_health = redis_client
        .get(format!("{}/ping", config.redis_url))
        .bearer_auth(&config.redis_token)
        .send()
        .await?
        .error_for_status()?;

    let ping: Value = redis_health.json().await?;

    if ping.get("result").and_then(Value::as_str) != Some("PONG") {
        return Err(format!("Unexpected Upstash PING response: {ping}").into());
    }

    info!(
        feeds = feeds.len(),
        poll_seconds = config.poll_interval.as_secs(),
        "GTFS fetcher started"
    );

    let mut ticker = interval(config.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received; stopping fetcher");
                break;
            }

            _ = ticker.tick() => {
                // All three feeds are requested concurrently.
                let results = join_all(feeds.iter().map(|feed| {
                    fetch_and_store(&fetch_client, &config, feed)
                })).await;

                for (feed, result) in feeds.iter().zip(results) {
                    if let Err(err) = result {
                        warn!(
                            feed = feed.name,
                            error = %err,
                            "Feed fetch or cache update failed"
                        );
                    }
                }
            }
        }
    }

    info!("GTFS fetcher stopped");
    Ok(())
}