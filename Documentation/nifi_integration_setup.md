# Apache NiFi Data Integration Pipeline Setup

## Overview
This document provides the complete setup and configuration for Apache NiFi data integration pipelines, demonstrating real-time streaming data processing from various sources including IoT protocols, APIs, and files.

## NiFi Flow Template Configuration

### 1. IoT Data Ingestion Flow

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<template encoding-version="1.2">
    <description>IoT Data Ingestion Pipeline with MQTT, SNMP, and HTTP sources</description>
    <groupId>iot-ingestion-group</groupId>
    <name>IoT Data Integration Pipeline</name>
    
    <!-- MQTT Consumer Processor -->
    <processor>
        <id>mqtt-consumer</id>
        <name>ConsumeMQTT</name>
        <class>org.apache.nifi.processors.mqtt.ConsumeMQTT</class>
        <properties>
            <property>
                <name>Broker URI</name>
                <value>tcp://localhost:1883</value>
            </property>
            <property>
                <name>Topic Filter</name>
                <value>sensors/+/data</value>
            </property>
            <property>
                <name>Quality of Service</name>
                <value>1</value>
            </property>
            <property>
                <name>Max Queue Size</name>
                <value>1000</value>
            </property>
        </properties>
    </processor>
    
    <!-- SNMP Processor -->
    <processor>
        <id>snmp-get</id>
        <name>GetSNMP</name>
        <class>org.apache.nifi.processors.snmp.GetSNMP</class>
        <properties>
            <property>
                <name>SNMP Host</name>
                <value>192.168.1.100</value>
            </property>
            <property>
                <name>SNMP Port</name>
                <value>161</value>
            </property>
            <property>
                <name>SNMP Community</name>
                <value>public</value>
            </property>
            <property>
                <name>OID</name>
                <value>1.3.6.1.2.1.1.1.0</value>
            </property>
        </properties>
        <schedulingStrategy>TIMER_DRIVEN</schedulingStrategy>
        <schedulingPeriod>30 sec</schedulingPeriod>
    </processor>
    
    <!-- HTTP API Listener -->
    <processor>
        <id>http-listener</id>
        <name>ListenHTTP</name>
        <class>org.apache.nifi.processors.standard.ListenHTTP</class>
        <properties>
            <property>
                <name>Listening Port</name>
                <value>8080</value>
            </property>
            <property>
                <name>Base Path</name>
                <value>iot</value>
            </property>
            <property>
                <name>HTTP Context Map</name>
                <value>http-context-map</value>
            </property>
        </properties>
    </processor>
    
    <!-- Data Transformation and Validation -->
    <processor>
        <id>jolt-transform</id>
        <name>JoltTransformJSON</name>
        <class>org.apache.nifi.processors.standard.JoltTransformJSON</class>
        <properties>
            <property>
                <name>Jolt Specification</name>
                <value>{
                    "operation": "shift",
                    "spec": {
                        "timestamp": "event_time",
                        "deviceId": "device_id",
                        "sensorData": {
                            "*": "metrics.&"
                        },
                        "location": "geo_location",
                        "*": "&"
                    }
                }</value>
            </property>
        </properties>
    </processor>
    
    <!-- Route Based on Data Quality -->
    <processor>
        <id>route-quality</id>
        <name>RouteOnAttribute</name>
        <class>org.apache.nifi.processors.standard.RouteOnAttribute</class>
        <properties>
            <property>
                <name>valid_data</name>
                <value>${json.path('$.device_id'):notNull():and(${json.path('$.event_time'):notNull()})}</value>
            </property>
            <property>
                <name>invalid_data</name>
                <value>${json.path('$.device_id'):isNull():or(${json.path('$.event_time'):isNull()})}</value>
            </property>
        </properties>
    </processor>
    
    <!-- Kafka Publisher -->
    <processor>
        <id>kafka-publisher</id>
        <name>PublishKafka_2_6</name>
        <class>org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6</class>
        <properties>
            <property>
                <name>bootstrap.servers</name>
                <value>localhost:9092</value>
            </property>
            <property>
                <name>Topic Name</name>
                <value>iot-streaming-data</value>
            </property>
            <property>
                <name>Delivery Guarantee</name>
                <value>1</value>
            </property>
            <property>
                <name>Key Attribute Encoding</name>
                <value>UTF-8</value>
            </property>
        </properties>
    </processor>
    
    <!-- Error Handling -->
    <processor>
        <id>log-errors</id>
        <name>LogMessage</name>
        <class>org.apache.nifi.processors.standard.LogMessage</class>
        <properties>
            <property>
                <name>Log Level</name>
                <value>error</value>
            </property>
            <property>
                <name>Log Message</name>
                <value>Data validation failed: ${filename}</value>
            </property>
        </properties>
    </processor>
    
    <!-- Connections -->
    <connection>
        <id>mqtt-to-transform</id>
        <sourceId>mqtt-consumer</sourceId>
        <destinationId>jolt-transform</destinationId>
        <relationship>success</relationship>
    </connection>
    
    <connection>
        <id>snmp-to-transform</id>
        <sourceId>snmp-get</sourceId>
        <destinationId>jolt-transform</destinationId>
        <relationship>success</relationship>
    </connection>
    
    <connection>
        <id>http-to-transform</id>
        <sourceId>http-listener</sourceId>
        <destinationId>jolt-transform</destinationId>
        <relationship>success</relationship>
    </connection>
    
    <connection>
        <id>transform-to-route</id>
        <sourceId>jolt-transform</sourceId>
        <destinationId>route-quality</destinationId>
        <relationship>success</relationship>
    </connection>
    
    <connection>
        <id>valid-to-kafka</id>
        <sourceId>route-quality</sourceId>
        <destinationId>kafka-publisher</destinationId>
        <relationship>valid_data</relationship>
    </connection>
    
    <connection>
        <id>invalid-to-log</id>
        <sourceId>route-quality</sourceId>
        <destinationId>log-errors</destinationId>
        <relationship>invalid_data</relationship>
    </connection>
</template>
```

## NiFi Configuration Steps

### 1. Docker Compose Setup
```yaml
version: '3.8'
services:
  nifi:
    image: apache/nifi:1.23.2
    ports:
      - "8443:8443"
      - "8080:8080"
    environment:
      - SINGLE_USER_CREDENTIALS_USERNAME=admin
      - SINGLE_USER_CREDENTIALS_PASSWORD=password123
      - NIFI_WEB_HTTPS_PORT=8443
      - NIFI_WEB_HTTP_PORT=8080
    volumes:
      - nifi_conf:/opt/nifi/nifi-current/conf
      - nifi_database_repository:/opt/nifi/nifi-current/database_repository
      - nifi_flowfile_repository:/opt/nifi/nifi-current/flowfile_repository
      - nifi_content_repository:/opt/nifi/nifi-current/content_repository
      - nifi_provenance_repository:/opt/nifi/nifi-current/provenance_repository
    networks:
      - data_integration

volumes:
  nifi_conf:
  nifi_database_repository:
  nifi_flowfile_repository:
  nifi_content_repository:
  nifi_provenance_repository:

networks:
  data_integration:
    driver: bridge
```

### 2. Custom Processors Configuration

#### MQTT Configuration
- **Broker URI**: tcp://localhost:1883
- **Topic Filter**: sensors/+/data (wildcard for multiple sensors)
- **QoS Level**: 1 (at least once delivery)
- **Max Queue Size**: 1000 messages
- **Session Expiry**: 30 seconds

#### SNMP Configuration
- **SNMP Version**: v2c
- **Community String**: public
- **Timeout**: 5 seconds
- **Retries**: 3
- **OID Patterns**: System info, interface statistics

#### Data Transformation Rules
```json
{
  "operation": "shift",
  "spec": {
    "timestamp": "event_time",
    "deviceId": "device_id",
    "temperature": "metrics.temperature",
    "humidity": "metrics.humidity",
    "pressure": "metrics.pressure",
    "location": {
      "lat": "geo_location.latitude",
      "lon": "geo_location.longitude"
    },
    "metadata": {
      "*": "device_metadata.&"
    }
  }
}
```

### 3. Performance Tuning Parameters

#### Processor Configuration
```properties
# Flow File Repository
nifi.flowfile.repository.implementation=org.apache.nifi.controller.repository.WriteAheadFlowFileRepository
nifi.flowfile.repository.wal.implementation=org.apache.nifi.controller.repository.SequentialAccessWriteAheadLog
nifi.flowfile.repository.directory=./flowfile_repository
nifi.flowfile.repository.checkpoint.interval=20 secs

# Content Repository
nifi.content.repository.implementation=org.apache.nifi.controller.repository.FileSystemRepository
nifi.content.repository.directory.default=./content_repository
nifi.content.repository.archive.max.retention.period=7 days
nifi.content.repository.archive.max.usage.percentage=50%

# Provenance Repository
nifi.provenance.repository.implementation=org.apache.nifi.provenance.WriteAheadProvenanceRepository
nifi.provenance.repository.directory.default=./provenance_repository
nifi.provenance.repository.max.storage.time=30 days
nifi.provenance.repository.max.storage.size=10 GB

# Component Status
nifi.components.status.repository.implementation=org.apache.nifi.controller.status.history.VolatileComponentStatusRepository
nifi.components.status.repository.buffer.size=1440
nifi.components.status.snapshot.frequency=1 min
```

## Installation and Deployment

### Prerequisites
```bash
# Java 11 or higher
java -version

# Docker and Docker Compose
docker --version
docker-compose --version

# Apache NiFi
wget https://archive.apache.org/dist/nifi/1.23.2/nifi-1.23.2-bin.tar.gz
```

### Setup Commands
```bash
# 1. Start NiFi with Docker
docker-compose up -d nifi

# 2. Access NiFi UI
# Navigate to: https://localhost:8443/nifi
# Username: admin
# Password: password123

# 3. Import Template
# Upload the XML template via NiFi UI
# Templates > Browse > Upload Template

# 4. Configure Controller Services
# Configure SSL Context Service for secure communications
# Configure Database Connection Pool for data persistence
```

### Monitoring and Alerts
```bash
# NiFi System Diagnostics API
curl -X GET "https://localhost:8443/nifi-api/system-diagnostics" \
  -H "Authorization: Bearer ${NIFI_TOKEN}"

# Flow Status Monitoring
curl -X GET "https://localhost:8443/nifi-api/flow/status" \
  -H "Authorization: Bearer ${NIFI_TOKEN}"

# Processor Statistics
curl -X GET "https://localhost:8443/nifi-api/processors/${PROCESSOR_ID}/status" \
  -H "Authorization: Bearer ${NIFI_TOKEN}"
```

## Data Flow Architecture

### High-Level Architecture
```
[IoT Devices] --MQTT--> [NiFi MQTT Consumer]
[Network Devices] --SNMP--> [NiFi SNMP Processor]
[External APIs] --HTTP--> [NiFi HTTP Listener]
                     |
                     v
              [Data Transformation]
                     |
                     v
              [Data Validation]
                     |
                     v
         [Route: Valid vs Invalid]
                  /        \
                 v          v
        [Kafka Publisher] [Error Log]
                 |
                 v
        [Downstream Analytics]
```

### Error Handling Strategy
1. **Connection Failures**: Automatic retry with exponential backoff
2. **Data Validation**: Route invalid data to error queue
3. **Transform Errors**: Log and alert on schema mismatches  
4. **Backpressure**: Configure queue limits and pressure release
5. **Dead Letter Queue**: Store unprocessable messages for analysis

## Performance Metrics
- **Throughput**: Target 10,000 messages/second
- **Latency**: < 100ms end-to-end processing
- **Availability**: 99.9% uptime SLA
- **Data Quality**: < 0.1% error rate

## Security Configuration
- SSL/TLS encryption for all communications
- Certificate-based authentication
- Role-based access control (RBAC)
- Data encryption at rest
- Audit logging for all operations

This NiFi setup provides a robust, scalable foundation for real-time data integration from multiple IoT and enterprise sources.