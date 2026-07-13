use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Settings{
    pub database: PostgresSettings,
    pub rabbitmq: RabbitmqSettings,
    pub worker: Worker
}

#[derive(Serialize, Deserialize)]
pub struct PostgresSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String
}

#[derive(Serialize, Deserialize)]
pub struct RabbitmqSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub vhost: String,
}

#[derive(Serialize, Deserialize)]
pub struct Worker {
    pub name: String,
}


