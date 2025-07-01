#!/usr/bin/env python3
"""
Data Quality and Validation Framework

Comprehensive data quality assurance for IoT streaming data pipeline.
Implements validation rules, quality metrics, and data profiling.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import re

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    quality_score: float
    metrics: Dict[str, Any]

class DataQualityValidator:
    """Comprehensive data quality validator"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rules = self._load_validation_rules()
    
    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules configuration"""
        return {
            'device_id': {
                'required': True,
                'type': str,
                'pattern': r'^[a-zA-Z0-9_-]+$',
                'min_length': 3,
                'max_length': 50
            },
            'timestamp': {
                'required': True,
                'type': str,
                'format': 'iso8601',
                'max_age_hours': 24
            },
            'temperature': {
                'type': float,
                'min_value': -50.0,
                'max_value': 100.0,
                'precision': 2
            },
            'humidity': {
                'type': float,
                'min_value': 0.0,
                'max_value': 100.0,
                'precision': 2
            },
            'pressure': {
                'type': float,
                'min_value': 800.0,
                'max_value': 1200.0,
                'precision': 2
            },
            'protocol': {
                'required': True,
                'type': str,
                'allowed_values': ['MQTT', 'SNMP', 'CoAP', 'TCP', 'WebSocket']
            }
        }
    
    def validate_message(self, message: Dict[str, Any]) -> ValidationResult:
        """Validate a single IoT message"""
        errors = []
        warnings = []
        quality_score = 100.0
        metrics = {}
        
        # Required field validation
        for field, rules in self.rules.items():
            if rules.get('required', False) and field not in message:
                errors.append(f"Missing required field: {field}")
                quality_score -= 20
        
        # Field-specific validation
        for field, value in message.items():
            if field in self.rules:
                field_result = self._validate_field(field, value, self.rules[field])
                errors.extend(field_result['errors'])
                warnings.extend(field_result['warnings'])
                quality_score -= field_result['penalty']
                metrics[f"{field}_quality"] = field_result['quality']
        
        # Cross-field validation
        cross_validation = self._validate_cross_fields(message)
        errors.extend(cross_validation['errors'])
        warnings.extend(cross_validation['warnings'])
        quality_score -= cross_validation['penalty']
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            quality_score=max(0, quality_score),
            metrics=metrics
        )
    
    def _validate_field(self, field_name: str, value: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validate individual field"""
        errors = []
        warnings = []
        penalty = 0
        quality = 100.0
        
        # Type validation
        expected_type = rules.get('type')
        if expected_type and not isinstance(value, expected_type):
            try:
                # Try to convert
                if expected_type == float:
                    value = float(value)
                elif expected_type == int:
                    value = int(value)
                elif expected_type == str:
                    value = str(value)
            except (ValueError, TypeError):
                errors.append(f"{field_name}: Invalid type. Expected {expected_type.__name__}")
                penalty += 15
                quality -= 30
        
        if isinstance(value, str):
            # String-specific validations
            if 'pattern' in rules:
                if not re.match(rules['pattern'], value):
                    errors.append(f"{field_name}: Does not match required pattern")
                    penalty += 10
                    quality -= 20
            
            if 'min_length' in rules and len(value) < rules['min_length']:
                errors.append(f"{field_name}: Too short (min: {rules['min_length']})")
                penalty += 5
                quality -= 15
            
            if 'max_length' in rules and len(value) > rules['max_length']:
                warnings.append(f"{field_name}: Too long (max: {rules['max_length']})")
                quality -= 5
            
            if 'allowed_values' in rules and value not in rules['allowed_values']:
                errors.append(f"{field_name}: Invalid value. Allowed: {rules['allowed_values']}")
                penalty += 10
                quality -= 25
            
            # Timestamp validation
            if rules.get('format') == 'iso8601':
                try:
                    parsed_time = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    max_age = rules.get('max_age_hours', 24)
                    if datetime.now() - parsed_time > timedelta(hours=max_age):
                        warnings.append(f"{field_name}: Timestamp is too old")
                        quality -= 10
                except ValueError:
                    errors.append(f"{field_name}: Invalid timestamp format")
                    penalty += 15
                    quality -= 30
        
        elif isinstance(value, (int, float)):
            # Numeric validations
            if 'min_value' in rules and value < rules['min_value']:
                errors.append(f"{field_name}: Below minimum ({rules['min_value']})")
                penalty += 10
                quality -= 20
            
            if 'max_value' in rules and value > rules['max_value']:
                errors.append(f"{field_name}: Above maximum ({rules['max_value']})")
                penalty += 10
                quality -= 20
            
            # Precision check
            if 'precision' in rules and isinstance(value, float):
                decimal_places = len(str(value).split('.')[-1]) if '.' in str(value) else 0
                if decimal_places > rules['precision']:
                    warnings.append(f"{field_name}: Too many decimal places")
                    quality -= 5
        
        return {
            'errors': errors,
            'warnings': warnings,
            'penalty': penalty,
            'quality': max(0, quality)
        }
    
    def _validate_cross_fields(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate relationships between fields"""
        errors = []
        warnings = []
        penalty = 0
        
        # Example cross-field validations
        
        # Temperature and humidity correlation
        if 'temperature' in message and 'humidity' in message:
            temp = float(message['temperature'])
            humidity = float(message['humidity'])
            
            # High temperature usually means lower humidity
            if temp > 30 and humidity > 80:
                warnings.append("Unusual: High temperature with high humidity")
            
            # Freezing temperature with high humidity is suspicious
            if temp < 0 and humidity > 90:
                warnings.append("Suspicious: Sub-zero temperature with very high humidity")
        
        # Protocol and data consistency
        protocol = message.get('protocol', '')
        if protocol == 'SNMP' and 'device_id' in message:
            # SNMP devices usually have IP-like identifiers
            device_id = message['device_id']
            if not re.match(r'^\d+\.\d+\.\d+\.\d+', device_id) and not re.match(r'^snmp_', device_id):
                warnings.append("SNMP device ID format may be incorrect")
        
        return {
            'errors': errors,
            'warnings': warnings,
            'penalty': penalty
        }


class DataProfiler:
    """Data profiling and statistics"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.profile_data = {}
    
    def profile_dataset(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive data profile"""
        if not messages:
            return {}
        
        profile = {
            'total_records': len(messages),
            'timestamp_range': self._analyze_timestamps(messages),
            'device_analysis': self._analyze_devices(messages),
            'protocol_distribution': self._analyze_protocols(messages),
            'data_quality_summary': self._analyze_quality(messages),
            'field_statistics': self._analyze_fields(messages)
        }
        
        return profile
    
    def _analyze_timestamps(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze timestamp patterns"""
        timestamps = []
        for msg in messages:
            if 'timestamp' in msg:
                try:
                    ts = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
                    timestamps.append(ts)
                except ValueError:
                    continue
        
        if not timestamps:
            return {}
        
        timestamps.sort()
        intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
        avg_interval = sum(intervals, timedelta()) / len(intervals) if intervals else timedelta()
        
        return {
            'earliest': timestamps[0].isoformat(),
            'latest': timestamps[-1].isoformat(),
            'span_hours': (timestamps[-1] - timestamps[0]).total_seconds() / 3600,
            'average_interval_seconds': avg_interval.total_seconds(),
            'total_timestamps': len(timestamps)
        }
    
    def _analyze_devices(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze device patterns"""
        devices = {}
        for msg in messages:
            device_id = msg.get('device_id', 'unknown')
            if device_id not in devices:
                devices[device_id] = {
                    'message_count': 0,
                    'protocols': set(),
                    'first_seen': None,
                    'last_seen': None
                }
            
            devices[device_id]['message_count'] += 1
            if 'protocol' in msg:
                devices[device_id]['protocols'].add(msg['protocol'])
            
            if 'timestamp' in msg:
                try:
                    ts = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
                    if devices[device_id]['first_seen'] is None or ts < devices[device_id]['first_seen']:
                        devices[device_id]['first_seen'] = ts
                    if devices[device_id]['last_seen'] is None or ts > devices[device_id]['last_seen']:
                        devices[device_id]['last_seen'] = ts
                except ValueError:
                    pass
        
        # Convert sets to lists for JSON serialization
        for device_info in devices.values():
            device_info['protocols'] = list(device_info['protocols'])
            if device_info['first_seen']:
                device_info['first_seen'] = device_info['first_seen'].isoformat()
            if device_info['last_seen']:
                device_info['last_seen'] = device_info['last_seen'].isoformat()
        
        return {
            'unique_devices': len(devices),
            'device_details': devices,
            'messages_per_device': {k: v['message_count'] for k, v in devices.items()}
        }
    
    def _analyze_protocols(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze protocol distribution"""
        protocols = {}
        for msg in messages:
            protocol = msg.get('protocol', 'unknown')
            protocols[protocol] = protocols.get(protocol, 0) + 1
        
        return protocols
    
    def _analyze_quality(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze overall data quality"""
        validator = DataQualityValidator()
        results = [validator.validate_message(msg) for msg in messages]
        
        valid_count = sum(1 for r in results if r.is_valid)
        total_errors = sum(len(r.errors) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)
        avg_quality_score = sum(r.quality_score for r in results) / len(results) if results else 0
        
        return {
            'valid_messages': valid_count,
            'invalid_messages': len(messages) - valid_count,
            'validation_rate': valid_count / len(messages) if messages else 0,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'average_quality_score': avg_quality_score
        }
    
    def _analyze_fields(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze field statistics"""
        field_stats = {}
        
        for msg in messages:
            for field, value in msg.items():
                if field not in field_stats:
                    field_stats[field] = {
                        'count': 0,
                        'null_count': 0,
                        'type_distribution': {},
                        'unique_values': set()
                    }
                
                field_stats[field]['count'] += 1
                
                if value is None:
                    field_stats[field]['null_count'] += 1
                else:
                    value_type = type(value).__name__
                    field_stats[field]['type_distribution'][value_type] = \
                        field_stats[field]['type_distribution'].get(value_type, 0) + 1
                    
                    # Track unique values (up to 100 for memory)
                    if len(field_stats[field]['unique_values']) < 100:
                        field_stats[field]['unique_values'].add(str(value))
        
        # Convert sets to lists and calculate additional metrics
        for field, stats in field_stats.items():
            stats['unique_values'] = list(stats['unique_values'])
            stats['uniqueness'] = len(stats['unique_values']) / stats['count'] if stats['count'] > 0 else 0
            stats['completeness'] = (stats['count'] - stats['null_count']) / stats['count'] if stats['count'] > 0 else 0
        
        return field_stats


def main():
    """Example usage and testing"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Data Quality and Validation Framework")
    print("=" * 50)
    
    # Example data
    sample_messages = [
        {
            "device_id": "sensor_001",
            "timestamp": "2024-01-15T10:30:00Z",
            "protocol": "MQTT",
            "temperature": 23.5,
            "humidity": 45.2,
            "pressure": 1013.25
        },
        {
            "device_id": "sensor_002", 
            "timestamp": "2024-01-15T10:31:00Z",
            "protocol": "SNMP",
            "temperature": 85.0,  # High temperature
            "humidity": 85.0,     # High humidity - suspicious combination
            "pressure": 1015.5
        },
        {
            "device_id": "",  # Invalid: empty device ID
            "timestamp": "invalid-timestamp",  # Invalid timestamp
            "protocol": "UNKNOWN",  # Invalid protocol
            "temperature": "not-a-number"  # Invalid type
        }
    ]
    
    # Validate messages
    validator = DataQualityValidator()
    print("\nValidation Results:")
    print("-" * 30)
    
    for i, msg in enumerate(sample_messages):
        result = validator.validate_message(msg)
        print(f"\nMessage {i+1}:")
        print(f"  Valid: {result.is_valid}")
        print(f"  Quality Score: {result.quality_score:.1f}%")
        if result.errors:
            print(f"  Errors: {result.errors}")
        if result.warnings:
            print(f"  Warnings: {result.warnings}")
    
    # Profile dataset
    profiler = DataProfiler()
    profile = profiler.profile_dataset(sample_messages)
    
    print("\nData Profile:")
    print("-" * 30)
    print(json.dumps(profile, indent=2, default=str))


if __name__ == "__main__":
    main()