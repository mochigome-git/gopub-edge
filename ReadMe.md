# MQTT to REST Data Pipeline for IoT Devices

This project is a Go-based data processing service that connects to an MQTT broker, listens to specified topics, parses incoming JSON messages, applies trigger-based processing logic, and patches or posts the transformed data to a RESTful API.

---

## 📦 Features

- Connects to secure MQTT brokers with TLS support.
- Reads MQTT payloads and unmarshals them into structured messages.
- Applies rule-based logic based on trigger devices and operational modes.
- Sends data to a RESTful API endpoint with support for various use cases:
  - Standard posting
  - Trigger-based patching
  - Data holding and batch combining
  - Special processing modes (e.g., weighing, filling, etc.)
- Thread-safe in-memory data storage for handling parallel operations.

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/mochigome-git/gopatch.git
cd gopatch
```

### 2. 🛠️Uncomment LoadEnv (local test)

```bash
sudo nano main.go

//utils.LoadEnv(".env.local")
↓
utils.LoadEnv(".env.local")
```

### 3. 🔧Environment Variables

#### Create a .env.local file with your configuration. Below is a minimal example:

https://github.com/mochigome-git/gopatch/blob/main/.env.example

```bash
# MQTT
MQTT_HOST=mqtt.example.com
MQTT_PORT=8883
MQTT_TOPIC="topic/+"
MQTTS_ON=true

ECS_MQTT_CA_CERTIFICATE="your-ca-cert"
ECS_MQTT_CLIENT_CERTIFICATE="your-client-cert"
ECS_MQTT_PRIVATE_KEY="your-client-key"

# API
API_URL="http://your-api-endpoint"
SERVICE_ROLE_KEY="your-service-role"
BASH_API="POST"

# Trigger
TRIGGER_DEVICE=d800,holdfillingweight
LOOPING=0.5

```

### 4. Run

```
go run main.go
```
