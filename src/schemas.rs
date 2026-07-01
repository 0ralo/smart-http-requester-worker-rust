use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct RabbitmqTask{
    pub task_id: Uuid
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Task {
    pub id: Uuid,
    pub url: String,
    pub method: String,
    pub headers: Option<serde_json::Value>,
    pub body: Option<String>,
    pub status: String,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub result: Option<serde_json::Value>,
}
