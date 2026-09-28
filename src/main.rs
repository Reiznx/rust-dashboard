use axum::{
    extract::{Multipart, Path, Query as AxumQuery},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use azure_core::credentials::Secret;
use azure_data_cosmos::{
    AccountEndpoint, AccountReference, CosmosClient, FeedScope, Query, RoutingStrategy,
};
use futures::TryStreamExt;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use tower_http::{cors::CorsLayer, services::ServeDir};

#[derive(Serialize, Deserialize, Clone)]
struct SensorData {
    id: Option<String>,
    device_id: String,
    firmware_version: Option<String>,
    suhu: f32,
    kelembapan: f32,
    status: String,
    received_at_ms: Option<u64>,
    network_ip: Option<String>,
    network_gateway: Option<String>,
    network_mask: Option<String>,
    wifi_connected: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_key: Option<String>,
}

#[tokio::main]
async fn main() {
    // Muat kredensial dari konfigurasi lokal yang tidak ikut di-commit.
    let project_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let bridge_env = project_dir.join("bridge.env");
    if let Err(error) = dotenvy::from_path(&bridge_env) {
        eprintln!("Could not load {}: {error}", bridge_env.display());
    }

    let app = Router::new()
        .route("/api/sensor", get(get_latest_sensor))
        .route("/api/sensors", get(get_sensor_history))
        .route("/api/report", get(get_report))
        .route("/api/firmware", post(upload_firmware))
        .route("/firmware/{filename}", get(download_firmware))
        .route("/api/records", get(get_records_page))
        .route("/api/records", delete(delete_record))
        .route("/api/records/all", delete(delete_all_records))
        .fallback_service(ServeDir::new(project_dir.join("static")))
        .layer(CorsLayer::permissive());

    tokio::spawn(run_mqtt_bridge());
    println!("MQTT bridge starting in the dashboard process...");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Dashboard aktif di http://127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}

async fn run_mqtt_bridge() {
    loop {
        if let Err(error) = run_mqtt_bridge_session().await {
            eprintln!("MQTT bridge stopped: {error}. Retrying in 5 seconds...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}

async fn run_mqtt_bridge_session() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint: AccountEndpoint = required_env("COSMOS_ENDPOINT")?.parse()?;
    let master_key = required_env("COSMOS_KEY")?;
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());

    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let client = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await?;
    let container_client = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await?;
    let container_properties = container_client.read(None).await?.into_model()?;
    let partition_key_path = container_properties
        .partition_key
        .paths()
        .first()
        .cloned()
        .ok_or("Cosmos container has no partition-key path")?;

    let mqtt_host = required_env("MQTT_HOST")?;
    let mqtt_port = required_env("MQTT_PORT")?.parse()?;
    let mqtt_user = required_env("MQTT_USER")?;
    let mqtt_password = required_env("MQTT_PASSWORD")?;
    let mqtt_topic = env::var("MQTT_TOPIC").unwrap_or_else(|_| "sample_enose".into());

    let mut mqtt_options = MqttOptions::new("rust-dashboard-bridge", mqtt_host, mqtt_port);
    mqtt_options.set_credentials(mqtt_user, mqtt_password);
    mqtt_options.set_keep_alive(std::time::Duration::from_secs(5));
    mqtt_options.set_transport(Transport::tls_with_default_config());

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    mqtt_client.subscribe(&mqtt_topic, QoS::AtMostOnce).await?;
    println!("MQTT bridge active: {mqtt_topic} -> Cosmos DB");

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                if let Err(error) =
                    save_mqtt_payload(&container_client, &partition_key_path, &publish.payload)
                        .await
                {
                    eprintln!("Could not save MQTT payload: {error}");
                }
            }
            Ok(_) => {}
            Err(error) => {
                return Err(format!("MQTT connection error: {error}").into());
            }
        }
    }
}

fn required_env(name: &str) -> Result<String, std::io::Error> {
    env::var(name).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("environment variable {name} is not set"),
        )
    })
}

async fn save_mqtt_payload(
    container_client: &azure_data_cosmos::ContainerClient,
    partition_key_path: &str,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut document = serde_json::from_slice::<serde_json::Value>(payload)?;
    let received_at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let device_id = document
        .get("device_id")
        .and_then(|value| value.as_str())
        .unwrap_or("default-device")
        .to_owned();
    let item_id = document
        .get("id")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{device_id}-{received_at_ms}"));

    let object = document
        .as_object_mut()
        .ok_or("MQTT payload must be a JSON object")?;
    object.insert("id".into(), serde_json::Value::String(item_id.clone()));
    object.insert(
        "received_at_ms".into(),
        serde_json::Value::Number(received_at_ms.into()),
    );

    let partition_key = document
        .pointer(partition_key_path)
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .or_else(|| {
            let field = partition_key_path
                .strip_prefix('/')
                .filter(|path| !path.is_empty() && !path.contains('/'))?;
            document.as_object_mut()?.insert(
                field.to_owned(),
                serde_json::Value::String(device_id.clone()),
            );
            Some(device_id.clone())
        })
        .ok_or("MQTT payload has no usable Cosmos partition key")?;

    container_client
        .create_item(partition_key, &item_id, &document, None)
        .await?;
    println!("Saved MQTT data for device {device_id}");
    Ok(())
}

#[derive(Serialize)]
struct FirmwareResponse {
    uploaded: bool,
    filename: Option<String>,
    sha256: Option<String>,
    device_id: Option<String>,
    message: String,
}

async fn upload_firmware(mut multipart: Multipart) -> Json<FirmwareResponse> {
    let mut firmware = None;
    let mut device_id = None;
    let mut version = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("firmware") => {
                let filename = field.file_name().unwrap_or("firmware.bin").to_owned();
                if !filename.to_ascii_lowercase().ends_with(".bin") {
                    return firmware_error("Only .bin firmware files are accepted.");
                }
                let Ok(bytes) = field.bytes().await else {
                    return firmware_error("Could not read the firmware file.");
                };
                firmware = Some((filename, bytes));
            }
            Some("device_id") => device_id = field.text().await.ok(),
            Some("version") => version = field.text().await.ok(),
            _ => {}
        }
    }

    let Some((filename, bytes)) = firmware else {
        return firmware_error("Choose a .bin firmware file first.");
    };
    if bytes.is_empty() || bytes.len() > 8 * 1024 * 1024 {
        return firmware_error("Firmware must be between 1 byte and 8 MB.");
    }
    let Some(device_id) = device_id.filter(|value| !value.is_empty()) else {
        return firmware_error("A target device_id is required.");
    };
    let Ok(base_url) = env::var("OTA_BASE_URL") else {
        return firmware_error("OTA_BASE_URL is not configured.");
    };
    if !base_url.starts_with("https://") {
        return firmware_error("OTA_BASE_URL must use HTTPS.");
    }

    let _ = tokio::fs::create_dir_all("firmware_uploads").await;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let stored_name = format!("{timestamp}-{filename}");
    let path = std::path::Path::new("firmware_uploads").join(&stored_name);
    if tokio::fs::write(path, &bytes).await.is_err() {
        return firmware_error("Could not stage the firmware file.");
    }

    let digest = format!("{:x}", Sha256::digest(&bytes));
    let firmware_url = format!("{}/firmware/{}", base_url.trim_end_matches('/'), stored_name);
    let command = serde_json::json!({
        "command": "ota_update",
        "device_id": device_id,
        "version": version.unwrap_or_else(|| "unknown".into()),
        "url": firmware_url,
        "size": bytes.len(),
        "sha256": digest,
    });
    if let Err(error) = publish_ota_command(&device_id, command).await {
        return firmware_error(&format!("Firmware staged but command failed: {error}"));
    }
    Json(FirmwareResponse {
        uploaded: true,
        filename: Some(stored_name),
        sha256: Some(digest),
        device_id: Some(device_id),
        message: "Firmware staged and OTA command sent to the device.".into(),
    })
}

fn firmware_error(message: &str) -> Json<FirmwareResponse> {
    Json(FirmwareResponse {
        uploaded: false,
        filename: None,
        sha256: None,
        device_id: None,
        message: message.into(),
    })
}

async fn download_firmware(Path(filename): Path<String>) -> Result<impl IntoResponse, StatusCode> {
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') || filename.contains("..") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let path = std::path::Path::new("firmware_uploads").join(filename);
    let bytes = tokio::fs::read(path).await.map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(([("content-type", "application/octet-stream")], bytes))
}

async fn publish_ota_command(
    device_id: &str,
    command: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let host = required_env("MQTT_HOST")?;
    let port = required_env("MQTT_PORT")?.parse()?;
    let user = required_env("MQTT_USER")?;
    let password = required_env("MQTT_PASSWORD")?;
    let mut options = MqttOptions::new("rust-dashboard-ota", host, port);
    options.set_credentials(user, password);
    options.set_keep_alive(std::time::Duration::from_secs(5));
    options.set_transport(Transport::tls_with_default_config());
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let topic = format!("devices/{device_id}/ota/command");
    client
        .publish(topic, QoS::AtLeastOnce, false, serde_json::to_vec(&command)?)
        .await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        eventloop.poll().await?;
        if tokio::time::Instant::now() + std::time::Duration::from_millis(100) >= deadline {
            break;
        }
    }
    Ok(())
}

async fn get_latest_sensor() -> Json<SensorData> {
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        eprintln!("COSMOS_ENDPOINT belum di-set");
        return fallback_sensor_data();
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        eprintln!("COSMOS_KEY belum di-set");
        return fallback_sensor_data();
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());

    let endpoint: AccountEndpoint = match endpoint.parse() {
        Ok(endpoint) => endpoint,
        Err(e) => {
            eprintln!("Endpoint Cosmos tidak valid: {e:?}");
            return fallback_sensor_data();
        }
    };

    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let client = match CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Gagal membuat Cosmos client: {e:?}");
            return fallback_sensor_data();
        }
    };

    let container_client = match client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal membuka container: {e:?}");
            return fallback_sensor_data();
        }
    };

    let query = "SELECT TOP 1 * FROM c ORDER BY c._ts DESC";
    let response = container_client
        .query_items::<SensorData>(Query::from(query), FeedScope::full_container(), None)
        .await;

    match response {
        Ok(mut stream) => {
            if let Ok(Some(data)) = stream.try_next().await {
                return Json(data);
            }
        }
        Err(e) => eprintln!("Query gagal: {e:?}"),
    }

    fallback_sensor_data()
}

async fn get_sensor_history() -> Json<Vec<SensorData>> {
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        return Json(Vec::new());
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        return Json(Vec::new());
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());
    let Ok(endpoint) = endpoint.parse::<AccountEndpoint>() else {
        return Json(Vec::new());
    };
    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let Ok(client) = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    else {
        return Json(Vec::new());
    };
    let Ok(container_client) = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    else {
        return Json(Vec::new());
    };
    let Ok(mut stream) = container_client
        .query_items::<SensorData>(
            Query::from("SELECT TOP 24 * FROM c ORDER BY c._ts DESC"),
            FeedScope::full_container(),
            None,
        )
        .await
    else {
        return Json(Vec::new());
    };

    let mut readings = Vec::new();
    while let Ok(Some(reading)) = stream.try_next().await {
        readings.push(reading);
    }
    readings.reverse();
    Json(readings)
}

#[derive(Deserialize)]
struct ReportQuery {
    hours: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
}

async fn get_report(AxumQuery(params): AxumQuery<ReportQuery>) -> Json<Vec<SensorData>> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default();
    let (start_ms, end_ms) = match (params.start_ms, params.end_ms) {
        (Some(start), Some(end)) if start < end => (start, end),
        _ => {
            let hours = params.hours.unwrap_or(1).clamp(1, 24);
            (now_ms.saturating_sub(hours * 60 * 60 * 1000), now_ms)
        }
    };
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        return Json(Vec::new());
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        return Json(Vec::new());
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());
    let Ok(endpoint) = endpoint.parse::<AccountEndpoint>() else {
        return Json(Vec::new());
    };
    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let Ok(client) = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    else {
        return Json(Vec::new());
    };
    let Ok(container_client) = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    else {
        return Json(Vec::new());
    };
    let query = format!(
        "SELECT * FROM c WHERE c.received_at_ms >= {start_ms} AND c.received_at_ms <= {end_ms} ORDER BY c.received_at_ms ASC"
    );
    let Ok(mut stream) = container_client
        .query_items::<SensorData>(Query::from(query), FeedScope::full_container(), None)
        .await
    else {
        return Json(Vec::new());
    };
    let mut records = Vec::new();
    while let Ok(Some(record)) = stream.try_next().await {
        records.push(record);
    }
    Json(records)
}

#[derive(Deserialize)]
struct RecordsPageQuery {
    page: Option<usize>,
}

#[derive(Serialize)]
struct RecordsPage {
    records: Vec<SensorData>,
    page: usize,
    has_next: bool,
}

async fn get_records_page(AxumQuery(params): AxumQuery<RecordsPageQuery>) -> Json<RecordsPage> {
    let page = params.page.unwrap_or(1).max(1);
    let offset = (page - 1) * 10;
    let empty = || {
        Json(RecordsPage {
            records: Vec::new(),
            page,
            has_next: false,
        })
    };
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        return empty();
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        return empty();
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());
    let Ok(endpoint) = endpoint.parse::<AccountEndpoint>() else {
        return empty();
    };
    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let Ok(client) = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    else {
        return empty();
    };
    let Ok(container_client) = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    else {
        return empty();
    };
    let Ok(response) = container_client.read(None).await else {
        return empty();
    };
    let Ok(properties) = response.into_model() else {
        return empty();
    };
    let partition_key_path = properties.partition_key.paths().first().cloned();
    let query = format!("SELECT * FROM c ORDER BY c._ts DESC OFFSET {offset} LIMIT 11");
    let Ok(mut stream) = container_client
        .query_items::<SensorData>(Query::from(query), FeedScope::full_container(), None)
        .await
    else {
        return empty();
    };
    let mut records = Vec::new();
    while let Ok(Some(record)) = stream.try_next().await {
        records.push(with_partition_key(record, partition_key_path.as_deref()));
    }
    let has_next = records.len() > 10;
    records.truncate(10);
    Json(RecordsPage {
        records,
        page,
        has_next,
    })
}

#[derive(Deserialize)]
struct DeleteRecordRequest {
    id: String,
    partition_key: String,
}

#[derive(Serialize)]
struct DeleteResponse {
    deleted: usize,
}

fn with_partition_key(mut record: SensorData, path: Option<&str>) -> SensorData {
    record.partition_key = match path {
        Some("/device_id") | Some("/enose_data") => Some(record.device_id.clone()),
        Some("/id") => record.id.clone(),
        _ => None,
    };
    record
}

async fn delete_record(Json(request): Json<DeleteRecordRequest>) -> Json<DeleteResponse> {
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());
    let Ok(endpoint) = endpoint.parse::<AccountEndpoint>() else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let Ok(client) = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(container_client) = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let deleted = container_client
        .delete_item(request.partition_key, &request.id, None)
        .await
        .map(|_| 1)
        .unwrap_or(0);
    Json(DeleteResponse { deleted })
}

async fn delete_all_records() -> Json<DeleteResponse> {
    let Ok(endpoint) = env::var("COSMOS_ENDPOINT") else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(master_key) = env::var("COSMOS_KEY") else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let database_name = env::var("COSMOS_DATABASE").unwrap_or_else(|_| "iot_project".into());
    let container_name = env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "enose_data".into());
    let region = env::var("COSMOS_REGION").unwrap_or_else(|_| "Southeast Asia".into());
    let Ok(endpoint) = endpoint.parse::<AccountEndpoint>() else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let account = AccountReference::with_authentication_key(endpoint, Secret::from(master_key));
    let Ok(client) = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region.into()))
        .await
    else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(container_client) = client
        .database_client(&database_name)
        .container_client(&container_name, None)
        .await
    else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(response) = container_client.read(None).await else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let Ok(properties) = response.into_model() else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let partition_key_path = properties.partition_key.paths().first().cloned();
    let Ok(mut stream) = container_client
        .query_items::<SensorData>(
            Query::from("SELECT * FROM c"),
            FeedScope::full_container(),
            None,
        )
        .await
    else {
        return Json(DeleteResponse { deleted: 0 });
    };
    let mut deleted = 0;
    while let Ok(Some(record)) = stream.try_next().await {
        let record = with_partition_key(record, partition_key_path.as_deref());
        if let (Some(id), Some(partition_key)) = (record.id, record.partition_key) {
            if container_client
                .delete_item(partition_key, &id, None)
                .await
                .is_ok()
            {
                deleted += 1;
            }
        }
    }
    Json(DeleteResponse { deleted })
}

fn fallback_sensor_data() -> Json<SensorData> {
    Json(SensorData {
        id: None,
        device_id: "waiting-data".to_string(),
        firmware_version: None,
        suhu: 0.0,
        kelembapan: 0.0,
        status: "No Data".to_string(),
        received_at_ms: None,
        network_ip: None,
        network_gateway: None,
        network_mask: None,
        wifi_connected: None,
        partition_key: None,
    })
}
