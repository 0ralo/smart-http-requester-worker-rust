use anyhow::bail;
use config::Config;
use lapin::{Channel, Connection, ConnectionProperties};
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions};
use lapin::types::FieldTable;
use lapin::uri::{AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use futures_util::stream::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value};
use sqlx::{ConnectOptions, Pool, Postgres};
use tracing::{error, info, warn};
use tracing::log::LevelFilter;
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;
use crate::configs::Settings;
use crate::schemas::{RabbitmqTask, Task};

pub mod configs;
mod schemas;

const DLX_EXCHANGE: &str = "tasks.dlx";
const RETRY_QUEUES: [&str; 7] = [
    "tasks.retry.1s",
    "tasks.retry.2s",
    "tasks.retry.4s",
    "tasks.retry.8s",
    "tasks.retry.16s",
    "tasks.retry.32s",
    "tasks.retry.64s",
];

fn config() -> anyhow::Result<Settings> {
    let settings = Config::builder()

        .set_default("redis.host", "127.0.0.1")?
        .set_default("redis.port", 6379)?

        .set_default("database.host", "127.0.0.1")?
        .set_default("database.port", 5432)?
        .set_default("database.database", "development")?
        .set_default("database.user", "dev")?

        .set_default("rabbitmq.host", "localhost")?
        .set_default("rabbitmq.port", 5672)?
        .set_default("rabbitmq.user", "guest")?
        .set_default("rabbitmq.password", "guest")?
        .set_default("rabbitmq.vhost", "")?

        .add_source(config::File::with_name("Settings"))
        .build()?
        .try_deserialize::<Settings>()?;

    Ok(settings)
}

#[tokio::main(worker_threads=4)]
async fn main() -> anyhow::Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
    let settings = config()?;
    let options = PgConnectOptions::new()
        .host(&settings.database.host)
        .port(settings.database.port)
        .username(&settings.database.user)
        .password(&settings.database.password)
        .database(&settings.database.database)
        .log_statements(LevelFilter::Info);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    let uri = AMQPUri {
        scheme: AMQPScheme::AMQP,
        authority: AMQPAuthority {
            userinfo: AMQPUserInfo {
                username: settings.rabbitmq.user,
                password: settings.rabbitmq.password,
            },
            host: settings.rabbitmq.host,
            port: settings.rabbitmq.port,
        },
        vhost: settings.rabbitmq.vhost,
        query: AMQPQueryString::default(),
    };
    let rabbitmq = Connection::connect_uri(uri, ConnectionProperties::default())
        .await?;
    let channel = rabbitmq.create_channel().await?;
    let mut consumer = channel
        .basic_consume(
            "tasks.queue".into(),
            "rust_consumer".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!("started");
    while let Some(delivery_result) = consumer.next().await {
        if let Ok(message) = delivery_result {
            info!("got message {:?}", String::from_utf8_lossy(&message.data));
            if let Err(e) = process_message(&channel, &pool, &message.data).await {
                error!("Cannot process message: {:?}", e);
                continue;
            }
            if let Err(e) = message.ack(BasicAckOptions::default()).await {
                error!("Failed to ack message: {:?}", e);
            }
            info!("Message processing completed");
        }
    }
    Ok(())
}

async fn send_to_retry_or_failed(
    channel: &Channel,
    task_id: &Uuid,
    attempt_count: i32,
    max_attempts: i32,
    error: Option<String>,
) -> anyhow::Result<()> {
    if attempt_count < max_attempts {
        let queue = RETRY_QUEUES
            .get((attempt_count - 1) as usize)
            .unwrap_or(&RETRY_QUEUES[RETRY_QUEUES.len() - 1]);

        let payload = serde_json::json!({ "task_id": task_id }).to_string();
        let msg = payload.as_bytes();

        channel
            .basic_publish(DLX_EXCHANGE.into(), queue.to_string().into(), BasicPublishOptions::default(), msg, Default::default())
                .await?;

        info!("Task {} sent to retry queue: {} (attempt {}/{})",
              task_id, queue, attempt_count + 1, max_attempts);
    } else {
        let payload = serde_json::json!({
            "task_id": task_id,
            "error": error.unwrap_or_else(|| "Unknown error".to_string()),
            "attempts": attempt_count
        }).to_string();

        let msg = payload.as_bytes();

        channel
            .basic_publish(DLX_EXCHANGE.into(), "failed".to_string().into(), BasicPublishOptions::default(), msg, Default::default())
            .await?;

        error!("Task {} sent to failed queue after {} attempts", task_id, attempt_count);
        bail!("Task is going to be dropped")
    }

    Ok(())
}

async fn process_message(channel: &Channel, pool: &Pool<Postgres>, message: &[u8]) -> anyhow::Result<()> {
    let data = std::str::from_utf8(message)?;
    let task_o = serde_json::from_str::<RabbitmqTask>(data)?;
    let mut tx = pool.begin().await?;
    let task = sqlx::query_as!(Task, "select id, url, method, headers, body, status, attempt_count, max_attempts, result from tasks where id = $1 for update skip locked", &task_o.task_id)
        .fetch_optional(&mut *tx)
        .await?;

    let Some(t) = task else {
        bail!("Failed to find task")
    };

    let result = make_request(t.url, t.method, t.headers, t.body).await;

    if let Ok(r) = result {
        sqlx::query!("update tasks set status='done', attempt_count = attempt_count + 1, updated_at=now(), result=jsonb_build_object('result', $1::text) where id = $2", r, &task_o.task_id)
            .execute(&mut *tx)
            .await?;
        info!("Task finished succesfully")
    } else {
        if let Err(e) = send_to_retry_or_failed(channel, &task_o.task_id, t.attempt_count+1, t.max_attempts, None).await {
            sqlx::query!("update tasks set updated_at=now(), status = 'failed', attempt_count = attempt_count + 1 where id = $1", &task_o.task_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query!("update tasks set updated_at=now(), attempt_count = attempt_count + 1 where id = $1", &task_o.task_id)
                .execute(&mut *tx)
                .await?;
        }
        warn!("Task wasn't finished succesfully");
    };
    tx.commit().await?;
    info!("Finished working on task");
    Ok(())
}


async fn make_request(
    url: String,
    method: String,
    headers: Option<Value>,
    body: Option<String>,
) -> anyhow::Result<String> {
    info!("Called make_request");
    let client = reqwest::Client::new();
    let headers = json_to_headermap(headers);
    let mut builder = match method.as_str() {
        "GET" => client.get(&url),
        "POST" => client.post(&url),
        "PUT" => client.put(&url),
        "DELETE" => client.delete(&url),
        "PATCH" => client.patch(&url),
        _ => bail!("Method not found")

    };
    builder = if let Ok(headers) = headers {
        builder.headers(headers)
    } else {
        builder
    };
    builder = if let Some(body) = body {
        builder.body(body)
    } else {
        builder
    };
    let data = builder.send().await?;
    Ok(data.text().await?)
}

fn json_to_headermap(json_headers: Option<Value>) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();

    let headers_obj = match json_headers {
        Some(Value::Object(obj)) => obj,
        Some(_) => bail!("Headers must be an object"),
        None => return Ok(headers),
    };

    for (key, value) in headers_obj {
        let header_value = match value {
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => continue,
        };

        let Ok(header_name) = HeaderName::from_bytes(key.as_bytes()) else {
            bail!("Invalid header name");
        };

        let Ok(header_value) = HeaderValue::from_str(&header_value) else {
            bail!("Invalid header value");
        };

        headers.insert(header_name, header_value);
    }

    Ok(headers)
}
