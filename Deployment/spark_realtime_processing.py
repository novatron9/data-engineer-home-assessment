#!/usr/bin/env python3
"""
Apache Spark Real-time Data Processing Pipeline

This module implements real-time data processing using Apache Spark Streaming
for IoT data analytics, anomaly detection, and real-time insights generation.

Features:
- Kafka integration for streaming data ingestion
- Real-time aggregations and window operations
- Machine learning for anomaly detection
- Complex event processing
- Performance monitoring and optimization
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import os
import sys

# Note: These would be installed via pip/conda in production
# pip install pyspark kafka-python numpy pandas scikit-learn

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.streaming import StreamingContext
    from pyspark.sql.streaming import StreamingQuery
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Spark not available: pip install pyspark")

try:
    import numpy as np
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    print("ML libraries not available: pip install numpy scikit-learn pandas")


@dataclass
class SparkConfig:
    """Spark configuration settings"""
    app_name: str = "IoT-RealTime-Processing"
    master: str = "local[*]"
    streaming_batch_interval: int = 5  # seconds
    checkpoint_location: str = "/tmp/spark-checkpoints"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topics: List[str] = None
    output_mode: str = "append"
    trigger_interval: str = "5 seconds"
    
    def __post_init__(self):
        if self.kafka_topics is None:
            self.kafka_topics = ["iot-streaming-data", "iot-processed-data"]


class SparkStreamingProcessor:
    """Main Spark streaming processor"""
    
    def __init__(self, config: SparkConfig):
        self.config = config
        self.spark = None
        self.streaming_queries: List[StreamingQuery] = []
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def initialize_spark(self) -> bool:
        """Initialize Spark session with optimized configuration"""
        if not SPARK_AVAILABLE:
            self.logger.error("Spark not available")
            return False
        
        try:
            # Spark configuration for streaming and Kafka
            spark_config = {
                "spark.app.name": self.config.app_name,
                "spark.master": self.config.master,
                "spark.sql.streaming.checkpointLocation": self.config.checkpoint_location,
                
                # Kafka integration
                "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
                
                # Performance optimizations
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
                
                # Memory management
                "spark.executor.memory": "2g",
                "spark.driver.memory": "1g",
                "spark.executor.cores": "2",
                
                # Streaming optimizations
                "spark.sql.streaming.minBatchesToRetain": "5",
                "spark.sql.streaming.stateStore.maintenanceInterval": "60s",
                
                # Serialization
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            }
            
            # Create Spark session
            builder = SparkSession.builder
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info("Spark session initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {e}")
            return False
    
    def create_kafka_stream(self, topics: List[str]) -> Optional[DataFrame]:
        """Create Kafka streaming DataFrame"""
        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
                .option("subscribe", ",".join(topics)) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse Kafka message structure
            parsed_df = kafka_df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), self._get_message_schema()).alias("message")
            ).select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("message.*")
            )
            
            self.logger.info(f"Created Kafka stream for topics: {topics}")
            return parsed_df
            
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            return None
    
    def _get_message_schema(self) -> StructType:
        """Define schema for incoming IoT messages"""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("data", StructType([
                StructField("device_id", StringType(), True),
                StructField("protocol", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
                StructField("metadata", MapType(StringType(), StringType()), True)
            ]), True)
        ])
    
    def process_iot_analytics(self, input_df: DataFrame) -> DataFrame:
        """Process IoT data for real-time analytics"""
        try:
            # Extract and flatten IoT data
            flattened_df = input_df.select(
                col("data.device_id").alias("device_id"),
                col("data.protocol").alias("protocol"),
                col("data.data").alias("sensor_data"),
                col("data.metadata").alias("metadata"),
                to_timestamp(col("data.timestamp")).alias("event_time"),
                current_timestamp().alias("processing_time")
            ).filter(col("device_id").isNotNull())
            
            # Add watermark for late data handling
            watermarked_df = flattened_df.withWatermark("event_time", "10 minutes")
            
            # Extract numeric metrics from sensor_data map
            metrics_df = watermarked_df.select(
                "*",
                # Convert string values to numeric where possible
                when(col("sensor_data.temperature").isNotNull(), 
                     col("sensor_data.temperature").cast("double")).alias("temperature"),
                when(col("sensor_data.humidity").isNotNull(), 
                     col("sensor_data.humidity").cast("double")).alias("humidity"),
                when(col("sensor_data.pressure").isNotNull(), 
                     col("sensor_data.pressure").cast("double")).alias("pressure"),
                when(col("sensor_data.vibration").isNotNull(), 
                     col("sensor_data.vibration").cast("double")).alias("vibration")
            )
            
            return metrics_df
            
        except Exception as e:
            self.logger.error(f"Error in IoT analytics processing: {e}")
            return input_df
    
    def create_windowed_aggregations(self, input_df: DataFrame) -> DataFrame:
        """Create windowed aggregations for real-time metrics"""
        try:
            # Define multiple window intervals
            windows = [
                {"duration": "1 minute", "slide": "30 seconds"},
                {"duration": "5 minutes", "slide": "1 minute"},
                {"duration": "15 minutes", "slide": "5 minutes"}
            ]
            
            aggregated_dfs = []
            
            for window_config in windows:
                window_df = input_df \
                    .groupBy(
                        window(col("event_time"), window_config["duration"], window_config["slide"]),
                        col("device_id"),
                        col("protocol")
                    ) \
                    .agg(
                        count("*").alias("message_count"),
                        avg("temperature").alias("avg_temperature"),
                        max("temperature").alias("max_temperature"),
                        min("temperature").alias("min_temperature"),
                        stddev("temperature").alias("stddev_temperature"),
                        avg("humidity").alias("avg_humidity"),
                        avg("pressure").alias("avg_pressure"),
                        avg("vibration").alias("avg_vibration"),
                        max("vibration").alias("max_vibration"),
                        first("processing_time").alias("last_processed")
                    ) \
                    .withColumn("window_duration", lit(window_config["duration"])) \
                    .withColumn("window_start", col("window.start")) \
                    .withColumn("window_end", col("window.end")) \
                    .drop("window")
                
                aggregated_dfs.append(window_df)
            
            # Union all window aggregations
            final_df = aggregated_dfs[0]
            for df in aggregated_dfs[1:]:
                final_df = final_df.union(df)
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error creating windowed aggregations: {e}")
            return input_df
    
    def detect_anomalies(self, input_df: DataFrame) -> DataFrame:
        """Detect anomalies in real-time data"""
        try:
            # Define anomaly detection rules
            anomaly_df = input_df.withColumn(
                "anomalies",
                array(
                    # Temperature anomalies
                    when((col("temperature") > 80) | (col("temperature") < -10), 
                         struct(lit("TEMPERATURE_EXTREME").alias("type"), 
                               col("temperature").alias("value"),
                               lit("Temperature outside normal range").alias("description"))),
                    
                    # Humidity anomalies
                    when((col("humidity") > 95) | (col("humidity") < 5), 
                         struct(lit("HUMIDITY_EXTREME").alias("type"), 
                               col("humidity").alias("value"),
                               lit("Humidity outside normal range").alias("description"))),
                    
                    # Vibration anomalies
                    when(col("vibration") > 100, 
                         struct(lit("HIGH_VIBRATION").alias("type"), 
                               col("vibration").alias("value"),
                               lit("Excessive vibration detected").alias("description"))),
                    
                    # Communication anomalies (based on message frequency)
                    when(col("message_count") < 5, 
                         struct(lit("LOW_FREQUENCY").alias("type"), 
                               col("message_count").alias("value"),
                               lit("Low message frequency detected").alias("description")))
                )
            ).filter(size(col("anomalies")) > 0)
            
            # Flatten anomalies and add metadata
            flattened_anomalies = anomaly_df.select(
                col("device_id"),
                col("protocol"),
                col("window_start"),
                col("window_end"),
                col("window_duration"),
                explode(col("anomalies")).alias("anomaly")
            ).select(
                "*",
                col("anomaly.type").alias("anomaly_type"),
                col("anomaly.value").alias("anomaly_value"),
                col("anomaly.description").alias("anomaly_description"),
                current_timestamp().alias("detected_at")
            ).drop("anomaly")
            
            return flattened_anomalies
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return input_df
    
    def create_device_health_scores(self, input_df: DataFrame) -> DataFrame:
        """Calculate device health scores based on multiple metrics"""
        try:
            health_df = input_df.withColumn(
                "health_score",
                # Normalize and weight different metrics
                (
                    # Temperature health (0-100, where 20-25°C is optimal)
                    when((col("avg_temperature") >= 20) & (col("avg_temperature") <= 25), 100)
                    .when((col("avg_temperature") >= 15) & (col("avg_temperature") <= 30), 80)
                    .when((col("avg_temperature") >= 10) & (col("avg_temperature") <= 35), 60)
                    .when((col("avg_temperature") >= 5) & (col("avg_temperature") <= 40), 40)
                    .otherwise(20) * 0.3
                ) +
                (
                    # Humidity health (0-100, where 40-60% is optimal)
                    when((col("avg_humidity") >= 40) & (col("avg_humidity") <= 60), 100)
                    .when((col("avg_humidity") >= 30) & (col("avg_humidity") <= 70), 80)
                    .when((col("avg_humidity") >= 20) & (col("avg_humidity") <= 80), 60)
                    .otherwise(40) * 0.2
                ) +
                (
                    # Vibration health (lower is better)
                    when(col("max_vibration") <= 10, 100)
                    .when(col("max_vibration") <= 25, 80)
                    .when(col("max_vibration") <= 50, 60)
                    .when(col("max_vibration") <= 75, 40)
                    .otherwise(20) * 0.3
                ) +
                (
                    # Communication health (based on message frequency)
                    when(col("message_count") >= 100, 100)
                    .when(col("message_count") >= 50, 80)
                    .when(col("message_count") >= 25, 60)
                    .when(col("message_count") >= 10, 40)
                    .otherwise(20) * 0.2
                )
            ).withColumn(
                "health_status",
                when(col("health_score") >= 90, "EXCELLENT")
                .when(col("health_score") >= 80, "GOOD")
                .when(col("health_score") >= 70, "FAIR")
                .when(col("health_score") >= 60, "POOR")
                .otherwise("CRITICAL")
            )
            
            return health_df
            
        except Exception as e:
            self.logger.error(f"Error calculating health scores: {e}")
            return input_df
    
    def setup_output_sinks(self, processed_df: DataFrame, anomalies_df: DataFrame, 
                          health_df: DataFrame) -> List[StreamingQuery]:
        """Set up output sinks for processed data"""
        queries = []
        
        try:
            # Console output for monitoring (development)
            console_query = processed_df.writeStream \
                .outputMode(self.config.output_mode) \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", "10") \
                .trigger(processingTime=self.config.trigger_interval) \
                .start()
            queries.append(console_query)
            
            # Kafka output for processed data
            kafka_processed_query = processed_df.select(
                to_json(struct("*")).alias("value")
            ).writeStream \
                .outputMode(self.config.output_mode) \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
                .option("topic", "iot-analytics-results") \
                .option("checkpointLocation", f"{self.config.checkpoint_location}/processed") \
                .trigger(processingTime=self.config.trigger_interval) \
                .start()
            queries.append(kafka_processed_query)
            
            # Kafka output for anomalies
            kafka_anomalies_query = anomalies_df.select(
                to_json(struct("*")).alias("value")
            ).writeStream \
                .outputMode(self.config.output_mode) \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
                .option("topic", "iot-anomalies") \
                .option("checkpointLocation", f"{self.config.checkpoint_location}/anomalies") \
                .trigger(processingTime=self.config.trigger_interval) \
                .start()
            queries.append(kafka_anomalies_query)
            
            # File output for health scores (for batch analysis)
            health_query = health_df.writeStream \
                .outputMode("append") \
                .format("json") \
                .option("path", "/tmp/spark-output/health-scores") \
                .option("checkpointLocation", f"{self.config.checkpoint_location}/health") \
                .trigger(processingTime="1 minute") \
                .start()
            queries.append(health_query)
            
            self.logger.info(f"Set up {len(queries)} output sinks")
            return queries
            
        except Exception as e:
            self.logger.error(f"Error setting up output sinks: {e}")
            return queries
    
    def run_streaming_pipeline(self):
        """Run the complete streaming pipeline"""
        try:
            if not self.initialize_spark():
                return False
            
            # Create input stream
            input_stream = self.create_kafka_stream(self.config.kafka_topics)
            if input_stream is None:
                return False
            
            # Process IoT analytics
            analytics_df = self.process_iot_analytics(input_stream)
            
            # Create windowed aggregations
            windowed_df = self.create_windowed_aggregations(analytics_df)
            
            # Detect anomalies
            anomalies_df = self.detect_anomalies(windowed_df)
            
            # Calculate health scores
            health_df = self.create_device_health_scores(windowed_df)
            
            # Set up output sinks
            self.streaming_queries = self.setup_output_sinks(windowed_df, anomalies_df, health_df)
            
            self.logger.info("Streaming pipeline started successfully")
            
            # Wait for termination
            for query in self.streaming_queries:
                query.awaitTermination()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error running streaming pipeline: {e}")
            return False
    
    def stop_pipeline(self):
        """Stop all streaming queries"""
        for query in self.streaming_queries:
            if query.isActive:
                query.stop()
        
        if self.spark:
            self.spark.stop()
        
        self.logger.info("Streaming pipeline stopped")


class MLAnomalyDetector:
    """Machine Learning-based anomaly detection"""
    
    def __init__(self):
        self.models = {}
        self.scalers = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def train_isolation_forest(self, data: List[Dict[str, float]], device_id: str):
        """Train Isolation Forest for anomaly detection"""
        if not ML_AVAILABLE:
            self.logger.warning("ML libraries not available")
            return
        
        try:
            # Prepare features
            features = []
            for record in data:
                feature_vector = [
                    record.get('temperature', 0),
                    record.get('humidity', 0),
                    record.get('pressure', 0),
                    record.get('vibration', 0)
                ]
                features.append(feature_vector)
            
            if len(features) < 10:  # Need minimum samples
                return
            
            # Scale features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features)
            
            # Train model
            model = IsolationForest(contamination=0.1, random_state=42)
            model.fit(scaled_features)
            
            # Store model and scaler
            self.models[device_id] = model
            self.scalers[device_id] = scaler
            
            self.logger.info(f"Trained anomaly detection model for device {device_id}")
            
        except Exception as e:
            self.logger.error(f"Error training ML model: {e}")
    
    def detect_anomaly(self, data: Dict[str, float], device_id: str) -> bool:
        """Detect anomaly using trained model"""
        if device_id not in self.models:
            return False
        
        try:
            # Prepare feature vector
            feature_vector = [[
                data.get('temperature', 0),
                data.get('humidity', 0),
                data.get('pressure', 0),
                data.get('vibration', 0)
            ]]
            
            # Scale and predict
            scaled_features = self.scalers[device_id].transform(feature_vector)
            prediction = self.models[device_id].predict(scaled_features)
            
            return prediction[0] == -1  # -1 indicates anomaly
            
        except Exception as e:
            self.logger.error(f"Error in ML anomaly detection: {e}")
            return False


class SparkPerformanceMonitor:
    """Monitor Spark streaming performance"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_streaming_metrics(self, query: StreamingQuery) -> Dict[str, Any]:
        """Get streaming query metrics"""
        try:
            progress = query.lastProgress
            if progress:
                return {
                    'query_id': progress.get('id'),
                    'name': progress.get('name'),
                    'timestamp': progress.get('timestamp'),
                    'batch_id': progress.get('batchId'),
                    'input_rows_per_second': progress.get('inputRowsPerSecond'),
                    'processed_rows_per_second': progress.get('processedRowsPerSecond'),
                    'batch_duration_ms': progress.get('durationMs', {}).get('triggerExecution'),
                    'state_store_memory_mb': progress.get('stateOperators', [{}])[0].get('memoryUsedBytes', 0) / 1024 / 1024
                }
            return {}
        except Exception as e:
            self.logger.error(f"Error getting streaming metrics: {e}")
            return {}
    
    def log_performance_metrics(self, queries: List[StreamingQuery]):
        """Log performance metrics for all queries"""
        for query in queries:
            if query.isActive:
                metrics = self.get_streaming_metrics(query)
                if metrics:
                    self.logger.info(f"Query {metrics['query_id']} - "
                                   f"Input: {metrics.get('input_rows_per_second', 0):.2f} rows/sec, "
                                   f"Processed: {metrics.get('processed_rows_per_second', 0):.2f} rows/sec, "
                                   f"Batch Duration: {metrics.get('batch_duration_ms', 0)}ms")


# Example configuration and usage
def create_example_config() -> SparkConfig:
    """Create example Spark configuration"""
    return SparkConfig(
        app_name="IoT-Data-Analytics",
        master="local[4]",
        streaming_batch_interval=5,
        checkpoint_location="/tmp/spark-checkpoints",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topics=["iot-streaming-data"],
        output_mode="update",
        trigger_interval="10 seconds"
    )


def main():
    """Main function for testing"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Spark Real-time Processing Pipeline")
    print("=" * 50)
    print("This implementation provides:")
    print("1. Real-time IoT data processing with Spark Streaming")
    print("2. Windowed aggregations and analytics")
    print("3. Anomaly detection (rule-based and ML)")
    print("4. Device health scoring")
    print("5. Multiple output sinks (Kafka, files, console)")
    print("6. Performance monitoring")
    
    if not SPARK_AVAILABLE:
        print("\nTo run this pipeline:")
        print("1. Install Spark: pip install pyspark")
        print("2. Start Kafka cluster")
        print("3. Configure Spark settings")
        print("4. Run the pipeline")
        return
    
    # Example usage
    config = create_example_config()
    processor = SparkStreamingProcessor(config)
    
    print(f"\nConfigured for:")
    print(f"- Kafka servers: {config.kafka_bootstrap_servers}")
    print(f"- Topics: {config.kafka_topics}")
    print(f"- Checkpoint: {config.checkpoint_location}")
    print(f"- Trigger interval: {config.trigger_interval}")
    
    print("\nTo start the pipeline:")
    print("processor.run_streaming_pipeline()")


if __name__ == "__main__":
    main()