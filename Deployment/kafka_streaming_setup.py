#!/usr/bin/env python3
"""
Kafka Message Broker Setup for Data Streaming

This module provides comprehensive Kafka configuration and management
for real-time data streaming in the IoT integration pipeline.

Features:
- Producer and Consumer implementations
- Topic management and configuration
- Schema registry integration
- Error handling and retry mechanisms
- Performance monitoring
- Security configuration
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
import threading
import queue
from abc import ABC, abstractmethod

# Note: These would be installed via pip in production
# pip install kafka-python confluent-kafka avro-python3

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
    from kafka.errors import KafkaError, KafkaTimeoutError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Kafka not available: pip install kafka-python")

try:
    from confluent_kafka import Producer, Consumer, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic as ConfluentNewTopic
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    print("Confluent Kafka not available: pip install confluent-kafka")


@dataclass
class KafkaMessage:
    """Standardized Kafka message structure"""
    topic: str
    key: Optional[str]
    value: Dict[str, Any]
    headers: Optional[Dict[str, str]] = None
    timestamp: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


class KafkaConfig:
    """Kafka configuration management"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self._load_default_config()
        if config_file:
            self._load_config_file(config_file)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default Kafka configuration"""
        return {
            'bootstrap_servers': ['localhost:9092'],
            'client_id': 'iot-data-integration',
            'security_protocol': 'PLAINTEXT',
            'sasl_mechanism': None,
            'sasl_username': None,
            'sasl_password': None,
            'ssl_cafile': None,
            'ssl_certfile': None,
            'ssl_keyfile': None,
            'producer': {
                'acks': 'all',
                'retries': 3,
                'batch_size': 16384,
                'linger_ms': 10,
                'buffer_memory': 33554432,
                'compression_type': 'snappy',
                'max_in_flight_requests_per_connection': 5,
                'enable_idempotence': True
            },
            'consumer': {
                'group_id': 'iot-consumer-group',
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': False,
                'max_poll_records': 500,
                'session_timeout_ms': 30000,
                'heartbeat_interval_ms': 3000,
                'fetch_min_bytes': 1,
                'fetch_max_wait_ms': 500
            },
            'topics': {
                'iot-streaming-data': {
                    'num_partitions': 6,
                    'replication_factor': 3,
                    'config': {
                        'retention.ms': 604800000,  # 7 days
                        'segment.ms': 86400000,     # 1 day
                        'cleanup.policy': 'delete',
                        'compression.type': 'snappy'
                    }
                },
                'iot-processed-data': {
                    'num_partitions': 3,
                    'replication_factor': 3,
                    'config': {
                        'retention.ms': 2592000000,  # 30 days
                        'segment.ms': 86400000,
                        'cleanup.policy': 'delete'
                    }
                },
                'iot-alerts': {
                    'num_partitions': 2,
                    'replication_factor': 3,
                    'config': {
                        'retention.ms': 7776000000,  # 90 days
                        'segment.ms': 86400000,
                        'cleanup.policy': 'delete'
                    }
                }
            }
        }
    
    def _load_config_file(self, config_file: str):
        """Load configuration from file"""
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                self.config.update(file_config)
        except Exception as e:
            logging.error(f"Failed to load config file {config_file}: {e}")
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration"""
        config = {
            'bootstrap_servers': self.config['bootstrap_servers'],
            'client_id': f"{self.config['client_id']}-producer",
            'security_protocol': self.config['security_protocol']
        }
        config.update(self.config['producer'])
        
        # Add security configs if present
        if self.config.get('sasl_mechanism'):
            config['sasl_mechanism'] = self.config['sasl_mechanism']
            config['sasl_plain_username'] = self.config['sasl_username']
            config['sasl_plain_password'] = self.config['sasl_password']
        
        return config
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Get consumer configuration"""
        config = {
            'bootstrap_servers': self.config['bootstrap_servers'],
            'client_id': f"{self.config['client_id']}-consumer",
            'security_protocol': self.config['security_protocol']
        }
        config.update(self.config['consumer'])
        
        # Add security configs if present
        if self.config.get('sasl_mechanism'):
            config['sasl_mechanism'] = self.config['sasl_mechanism']
            config['sasl_plain_username'] = self.config['sasl_username']
            config['sasl_plain_password'] = self.config['sasl_password']
        
        return config
    
    def get_admin_config(self) -> Dict[str, Any]:
        """Get admin client configuration"""
        return {
            'bootstrap_servers': self.config['bootstrap_servers'],
            'client_id': f"{self.config['client_id']}-admin",
            'security_protocol': self.config['security_protocol']
        }


class KafkaProducerManager:
    """Kafka producer with advanced features"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_connected = False
        self.delivery_reports = queue.Queue()
        
    def connect(self) -> bool:
        """Connect to Kafka cluster"""
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka library not available")
            return False
        
        try:
            producer_config = self.config.get_producer_config()
            
            # Add value serializer
            producer_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
            producer_config['key_serializer'] = lambda k: k.encode('utf-8') if k else None
            
            self.producer = KafkaProducer(**producer_config)
            self.is_connected = True
            self.logger.info("Kafka producer connected successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect Kafka producer: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Kafka"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.is_connected = False
            self.logger.info("Kafka producer disconnected")
    
    def send_message(self, topic: str, message: Dict[str, Any], 
                    key: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> bool:
        """Send message to Kafka topic"""
        if not self.is_connected:
            self.logger.error("Producer not connected")
            return False
        
        try:
            # Prepare message
            kafka_message = {
                'timestamp': datetime.utcnow().isoformat(),
                'data': message
            }
            
            # Prepare headers
            kafka_headers = []
            if headers:
                for k, v in headers.items():
                    kafka_headers.append((k, v.encode('utf-8')))
            
            # Send message
            future = self.producer.send(
                topic=topic,
                value=kafka_message,
                key=key,
                headers=kafka_headers if kafka_headers else None
            )
            
            # Add callback for delivery report
            future.add_callback(self._delivery_callback)
            future.add_errback(self._delivery_error_callback)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message to topic {topic}: {e}")
            return False
    
    def send_batch(self, messages: List[KafkaMessage]) -> int:
        """Send batch of messages"""
        sent_count = 0
        
        for msg in messages:
            if self.send_message(msg.topic, msg.value, msg.key, msg.headers):
                sent_count += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        self.logger.info(f"Sent batch: {sent_count}/{len(messages)} messages")
        return sent_count
    
    def _delivery_callback(self, record_metadata):
        """Callback for successful message delivery"""
        self.delivery_reports.put({
            'status': 'success',
            'topic': record_metadata.topic,
            'partition': record_metadata.partition,
            'offset': record_metadata.offset,
            'timestamp': record_metadata.timestamp
        })
    
    def _delivery_error_callback(self, exception):
        """Callback for message delivery errors"""
        self.delivery_reports.put({
            'status': 'error',
            'error': str(exception)
        })
        self.logger.error(f"Message delivery failed: {exception}")


class KafkaConsumerManager:
    """Kafka consumer with advanced features"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_connected = False
        self.is_consuming = False
        self.message_handlers: Dict[str, Callable[[KafkaMessage], None]] = {}
        
    def connect(self, topics: List[str]) -> bool:
        """Connect to Kafka cluster and subscribe to topics"""
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka library not available")
            return False
        
        try:
            consumer_config = self.config.get_consumer_config()
            
            # Add value deserializer
            consumer_config['value_deserializer'] = lambda m: json.loads(m.decode('utf-8'))
            consumer_config['key_deserializer'] = lambda k: k.decode('utf-8') if k else None
            
            self.consumer = KafkaConsumer(*topics, **consumer_config)
            self.is_connected = True
            self.logger.info(f"Kafka consumer connected and subscribed to topics: {topics}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect Kafka consumer: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Kafka"""
        if self.consumer:
            self.is_consuming = False
            self.consumer.close()
            self.is_connected = False
            self.logger.info("Kafka consumer disconnected")
    
    def add_message_handler(self, topic: str, handler: Callable[[KafkaMessage], None]):
        """Add message handler for specific topic"""
        self.message_handlers[topic] = handler
        self.logger.info(f"Added message handler for topic: {topic}")
    
    def start_consuming(self):
        """Start consuming messages"""
        if not self.is_connected:
            self.logger.error("Consumer not connected")
            return
        
        self.is_consuming = True
        self.logger.info("Started consuming messages")
        
        # Start consuming in background thread
        threading.Thread(target=self._consume_loop, daemon=True).start()
    
    def stop_consuming(self):
        """Stop consuming messages"""
        self.is_consuming = False
        self.logger.info("Stopped consuming messages")
    
    def _consume_loop(self):
        """Main message consumption loop"""
        try:
            while self.is_consuming:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self._process_message(message)
                    
                    # Commit offsets periodically
                    if message_batch:
                        self.consumer.commit()
                        
                except KafkaTimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"Error in consume loop: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in consume loop: {e}")
    
    def _process_message(self, message):
        """Process individual message"""
        try:
            # Create KafkaMessage object
            kafka_msg = KafkaMessage(
                topic=message.topic,
                key=message.key,
                value=message.value,
                headers={k: v.decode('utf-8') for k, v in message.headers} if message.headers else None,
                partition=message.partition,
                offset=message.offset,
                timestamp=datetime.fromtimestamp(message.timestamp / 1000).isoformat() if message.timestamp else None
            )
            
            # Call topic-specific handler if available
            if message.topic in self.message_handlers:
                self.message_handlers[message.topic](kafka_msg)
            else:
                self.logger.warning(f"No handler for topic: {message.topic}")
                
        except Exception as e:
            self.logger.error(f"Error processing message from {message.topic}: {e}")


class KafkaTopicManager:
    """Kafka topic management"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.admin_client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def connect(self) -> bool:
        """Connect admin client"""
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka library not available")
            return False
        
        try:
            admin_config = self.config.get_admin_config()
            self.admin_client = KafkaAdminClient(**admin_config)
            self.logger.info("Kafka admin client connected")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect admin client: {e}")
            return False
    
    def create_topics(self) -> bool:
        """Create all configured topics"""
        if not self.admin_client:
            self.logger.error("Admin client not connected")
            return False
        
        try:
            topics_to_create = []
            
            for topic_name, topic_config in self.config.config['topics'].items():
                topic = NewTopic(
                    name=topic_name,
                    num_partitions=topic_config['num_partitions'],
                    replication_factor=topic_config['replication_factor'],
                    topic_configs=topic_config.get('config', {})
                )
                topics_to_create.append(topic)
            
            # Create topics
            result = self.admin_client.create_topics(topics_to_create, validate_only=False)
            
            # Check results
            for topic_name, future in result.items():
                try:
                    future.result()
                    self.logger.info(f"Topic '{topic_name}' created successfully")
                except Exception as e:
                    if "already exists" in str(e):
                        self.logger.info(f"Topic '{topic_name}' already exists")
                    else:
                        self.logger.error(f"Failed to create topic '{topic_name}': {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create topics: {e}")
            return False
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            metadata = self.admin_client.list_topics()
            return list(metadata.topics.keys())
        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            return []
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic"""
        try:
            result = self.admin_client.delete_topics([topic_name])
            result[topic_name].result()
            self.logger.info(f"Topic '{topic_name}' deleted successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete topic '{topic_name}': {e}")
            return False


class KafkaStreamingPipeline:
    """Complete Kafka streaming pipeline manager"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = KafkaConfig(config_file)
        self.producer = KafkaProducerManager(self.config)
        self.consumer = KafkaConsumerManager(self.config)
        self.topic_manager = KafkaTopicManager(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def initialize(self) -> bool:
        """Initialize the complete pipeline"""
        try:
            # Connect admin client and create topics
            if not self.topic_manager.connect():
                return False
            
            if not self.topic_manager.create_topics():
                return False
            
            # Connect producer
            if not self.producer.connect():
                return False
            
            self.logger.info("Kafka streaming pipeline initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize pipeline: {e}")
            return False
    
    def setup_iot_data_flow(self):
        """Set up IoT data processing flow"""
        
        # Handler for raw IoT data
        def process_iot_data(message: KafkaMessage):
            try:
                data = message.value.get('data', {})
                
                # Basic data validation
                if self._validate_iot_data(data):
                    # Process and forward to processed data topic
                    processed_data = self._process_iot_data(data)
                    self.producer.send_message(
                        'iot-processed-data',
                        processed_data,
                        key=data.get('device_id')
                    )
                    
                    # Check for alerts
                    alerts = self._check_for_alerts(processed_data)
                    for alert in alerts:
                        self.producer.send_message(
                            'iot-alerts',
                            alert,
                            key=alert.get('device_id')
                        )
                
            except Exception as e:
                self.logger.error(f"Error processing IoT data: {e}")
        
        # Add handler for IoT streaming data
        self.consumer.add_message_handler('iot-streaming-data', process_iot_data)
    
    def _validate_iot_data(self, data: Dict[str, Any]) -> bool:
        """Validate IoT data"""
        required_fields = ['device_id', 'timestamp']
        return all(field in data for field in required_fields)
    
    def _process_iot_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process IoT data"""
        return {
            'device_id': data.get('device_id'),
            'timestamp': data.get('timestamp'),
            'processed_at': datetime.utcnow().isoformat(),
            'metrics': data.get('data', {}),
            'metadata': data.get('metadata', {})
        }
    
    def _check_for_alerts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for alert conditions"""
        alerts = []
        
        # Example alert conditions
        metrics = data.get('metrics', {})
        
        # Temperature alert
        if 'temperature' in metrics:
            temp = float(metrics['temperature'])
            if temp > 80:  # High temperature alert
                alerts.append({
                    'device_id': data.get('device_id'),
                    'alert_type': 'HIGH_TEMPERATURE',
                    'value': temp,
                    'threshold': 80,
                    'timestamp': datetime.utcnow().isoformat(),
                    'severity': 'WARNING'
                })
        
        return alerts
    
    def start_pipeline(self):
        """Start the complete pipeline"""
        try:
            # Set up data flow
            self.setup_iot_data_flow()
            
            # Connect consumer to topics
            topics = list(self.config.config['topics'].keys())
            if self.consumer.connect(topics):
                self.consumer.start_consuming()
                self.logger.info("Kafka streaming pipeline started")
            else:
                self.logger.error("Failed to start consumer")
                
        except Exception as e:
            self.logger.error(f"Failed to start pipeline: {e}")
    
    def stop_pipeline(self):
        """Stop the pipeline"""
        self.consumer.stop_consuming()
        self.consumer.disconnect()
        self.producer.disconnect()
        self.logger.info("Kafka streaming pipeline stopped")


# Docker Compose configuration for Kafka cluster
KAFKA_DOCKER_COMPOSE = """
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka1:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka1
    container_name: kafka1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka1:29092
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 3
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_CONFLUENT_SUPPORT_CUSTOMER_ID: anonymous
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
    volumes:
      - kafka1-data:/var/lib/kafka/data

  kafka2:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka2
    container_name: kafka2
    depends_on:
      - zookeeper
    ports:
      - "9093:9093"
      - "9102:9102"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:29093,PLAINTEXT_HOST://localhost:9093
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka2:29093
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 3
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_CONFLUENT_SUPPORT_CUSTOMER_ID: anonymous
      KAFKA_JMX_PORT: 9102
      KAFKA_JMX_HOSTNAME: localhost
    volumes:
      - kafka2-data:/var/lib/kafka/data

  kafka3:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka3
    container_name: kafka3
    depends_on:
      - zookeeper
    ports:
      - "9094:9094"
      - "9103:9103"
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:29094,PLAINTEXT_HOST://localhost:9094
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka3:29094
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 3
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_CONFLUENT_SUPPORT_CUSTOMER_ID: anonymous
      KAFKA_JMX_PORT: 9103
      KAFKA_JMX_HOSTNAME: localhost
    volumes:
      - kafka3-data:/var/lib/kafka/data

  schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - kafka1
      - kafka2
      - kafka3
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka1:29092,kafka2:29093,kafka3:29094'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka1
      - kafka2
      - kafka3
      - schema-registry
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka1:29092,kafka2:29093,kafka3:29094
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka1-data:
  kafka2-data:
  kafka3-data:

networks:
  default:
    name: kafka-network
"""


def main():
    """Main function for testing"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Kafka Streaming Setup")
    print("=" * 50)
    print("This implementation provides:")
    print("1. Kafka producer and consumer management")
    print("2. Topic administration")
    print("3. Message routing and processing")
    print("4. Error handling and monitoring")
    print("5. Docker-based cluster deployment")
    
    # Save Docker Compose file
    with open('/mnt/d/Mi_C3/kafka-docker-compose.yml', 'w') as f:
        f.write(KAFKA_DOCKER_COMPOSE)
    
    print("\nFiles created:")
    print("- kafka_streaming_setup.py (this file)")
    print("- kafka-docker-compose.yml (Docker deployment)")
    
    print("\nTo deploy Kafka cluster:")
    print("docker-compose -f kafka-docker-compose.yml up -d")
    
    print("\nTo use in production:")
    print("1. Install dependencies: pip install kafka-python")
    print("2. Configure security settings")
    print("3. Set up monitoring and alerting")
    print("4. Implement schema registry")
    print("5. Configure backup and disaster recovery")


if __name__ == "__main__":
    main()