#!/usr/bin/env python3
"""
Performance Monitoring and Alerting System

Comprehensive monitoring for the IoT data integration pipeline.
Monitors system health, performance metrics, and generates alerts.
"""

import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import queue
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class Alert:
    """Alert structure"""
    id: str
    timestamp: str
    severity: AlertSeverity
    component: str
    message: str
    details: Dict[str, Any]
    resolved: bool = False
    resolved_at: Optional[str] = None

@dataclass
class MetricPoint:
    """Single metric measurement"""
    timestamp: str
    value: float
    tags: Dict[str, str]

class MetricsCollector:
    """Collects and stores performance metrics"""
    
    def __init__(self):
        self.metrics: Dict[str, List[MetricPoint]] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def record_metric(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record a metric value"""
        if tags is None:
            tags = {}
        
        metric_point = MetricPoint(
            timestamp=datetime.utcnow().isoformat(),
            value=value,
            tags=tags
        )
        
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = []
            
            self.metrics[name].append(metric_point)
            
            # Keep only last 1000 points per metric
            if len(self.metrics[name]) > 1000:
                self.metrics[name] = self.metrics[name][-1000:]
    
    def get_metrics(self, name: str, since: Optional[datetime] = None) -> List[MetricPoint]:
        """Get metric values"""
        with self.lock:
            if name not in self.metrics:
                return []
            
            if since is None:
                return self.metrics[name].copy()
            
            since_str = since.isoformat()
            return [mp for mp in self.metrics[name] if mp.timestamp >= since_str]
    
    def get_metric_summary(self, name: str, window_minutes: int = 60) -> Dict[str, float]:
        """Get metric summary statistics"""
        since = datetime.utcnow() - timedelta(minutes=window_minutes)
        points = self.get_metrics(name, since)
        
        if not points:
            return {}
        
        values = [p.value for p in points]
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
            'latest': values[-1] if values else 0
        }


class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self):
        self.alerts: Dict[str, Alert] = {}
        self.alert_handlers: List[Callable[[Alert], None]] = []
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def add_alert_handler(self, handler: Callable[[Alert], None]):
        """Add alert handler"""
        self.alert_handlers.append(handler)
    
    def create_alert(self, component: str, message: str, severity: AlertSeverity, 
                    details: Dict[str, Any] = None) -> str:
        """Create new alert"""
        if details is None:
            details = {}
        
        alert_id = f"{component}_{int(time.time())}"
        alert = Alert(
            id=alert_id,
            timestamp=datetime.utcnow().isoformat(),
            severity=severity,
            component=component,
            message=message,
            details=details
        )
        
        with self.lock:
            self.alerts[alert_id] = alert
        
        # Notify handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                self.logger.error(f"Alert handler error: {e}")
        
        self.logger.log(
            self._severity_to_log_level(severity),
            f"Alert created: {component} - {message}"
        )
        
        return alert_id
    
    def resolve_alert(self, alert_id: str):
        """Resolve an alert"""
        with self.lock:
            if alert_id in self.alerts:
                self.alerts[alert_id].resolved = True
                self.alerts[alert_id].resolved_at = datetime.utcnow().isoformat()
                self.logger.info(f"Alert resolved: {alert_id}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts"""
        with self.lock:
            return [alert for alert in self.alerts.values() if not alert.resolved]
    
    def _severity_to_log_level(self, severity: AlertSeverity) -> int:
        """Convert alert severity to logging level"""
        mapping = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.ERROR: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL
        }
        return mapping.get(severity, logging.INFO)


class SystemMonitor:
    """Monitors system components and generates alerts"""
    
    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics = metrics_collector
        self.alerts = alert_manager
        self.monitoring_rules = self._load_monitoring_rules()
        self.is_running = False
        self.monitor_thread = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _load_monitoring_rules(self) -> Dict[str, Dict[str, Any]]:
        """Load monitoring rules configuration"""
        return {
            'kafka_lag': {
                'metric': 'kafka.consumer.lag',
                'threshold': 1000,
                'operator': '>',
                'severity': AlertSeverity.WARNING,
                'message': 'Kafka consumer lag is high'
            },
            'message_rate_low': {
                'metric': 'messages.per_second',
                'threshold': 10,
                'operator': '<',
                'severity': AlertSeverity.WARNING,
                'message': 'Message ingestion rate is low'
            },
            'error_rate_high': {
                'metric': 'errors.per_minute',
                'threshold': 5,
                'operator': '>',
                'severity': AlertSeverity.ERROR,
                'message': 'Error rate is too high'
            },
            'memory_usage_high': {
                'metric': 'system.memory.usage_percent',
                'threshold': 85,
                'operator': '>',
                'severity': AlertSeverity.WARNING,
                'message': 'Memory usage is high'
            },
            'disk_space_low': {
                'metric': 'system.disk.usage_percent',
                'threshold': 90,
                'operator': '>',
                'severity': AlertSeverity.CRITICAL,
                'message': 'Disk space is critically low'
            }
        }
    
    def start_monitoring(self):
        """Start monitoring thread"""
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.logger.info("System monitoring started")
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        self.logger.info("System monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                self._check_monitoring_rules()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)
    
    def _check_monitoring_rules(self):
        """Check all monitoring rules"""
        for rule_name, rule_config in self.monitoring_rules.items():
            try:
                self._check_rule(rule_name, rule_config)
            except Exception as e:
                self.logger.error(f"Error checking rule {rule_name}: {e}")
    
    def _check_rule(self, rule_name: str, rule_config: Dict[str, Any]):
        """Check individual monitoring rule"""
        metric_name = rule_config['metric']
        threshold = rule_config['threshold']
        operator = rule_config['operator']
        severity = rule_config['severity']
        message = rule_config['message']
        
        # Get recent metric summary
        summary = self.metrics.get_metric_summary(metric_name, window_minutes=5)
        if not summary:
            return
        
        current_value = summary['latest']
        
        # Check threshold
        threshold_exceeded = False
        if operator == '>':
            threshold_exceeded = current_value > threshold
        elif operator == '<':
            threshold_exceeded = current_value < threshold
        elif operator == '>=':
            threshold_exceeded = current_value >= threshold
        elif operator == '<=':
            threshold_exceeded = current_value <= threshold
        elif operator == '==':
            threshold_exceeded = current_value == threshold
        
        if threshold_exceeded:
            # Create alert
            details = {
                'metric': metric_name,
                'current_value': current_value,
                'threshold': threshold,
                'operator': operator,
                'summary': summary
            }
            
            self.alerts.create_alert(
                component=f"monitor.{rule_name}",
                message=f"{message} (current: {current_value}, threshold: {threshold})",
                severity=severity,
                details=details
            )


class PerformanceTracker:
    """Tracks performance of pipeline components"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_timers: Dict[str, float] = {}
    
    def start_timer(self, operation: str, tags: Dict[str, str] = None) -> str:
        """Start timing an operation"""
        timer_id = f"{operation}_{int(time.time() * 1000)}"
        self.active_timers[timer_id] = time.time()
        return timer_id
    
    def end_timer(self, timer_id: str, tags: Dict[str, str] = None):
        """End timing and record metric"""
        if timer_id not in self.active_timers:
            return
        
        start_time = self.active_timers.pop(timer_id)
        duration_ms = (time.time() - start_time) * 1000
        
        # Extract operation name from timer_id
        operation = timer_id.rsplit('_', 1)[0]
        
        self.metrics.record_metric(
            f"performance.{operation}.duration_ms",
            duration_ms,
            tags or {}
        )
    
    def record_throughput(self, component: str, count: int, tags: Dict[str, str] = None):
        """Record throughput metric"""
        self.metrics.record_metric(
            f"throughput.{component}.messages_per_second",
            count,
            tags or {}
        )
    
    def record_error(self, component: str, error_type: str, tags: Dict[str, str] = None):
        """Record error metric"""
        if tags is None:
            tags = {}
        tags['error_type'] = error_type
        
        self.metrics.record_metric(
            f"errors.{component}.count",
            1,
            tags
        )


class EmailNotifier:
    """Email notification handler"""
    
    def __init__(self, smtp_server: str, smtp_port: int, username: str, password: str,
                 recipients: List[str]):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.recipients = recipients
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def send_alert(self, alert: Alert):
        """Send alert via email"""
        try:
            subject = f"[{alert.severity.value}] {alert.component}: {alert.message}"
            
            body = f"""
Alert Details:
- ID: {alert.id}
- Timestamp: {alert.timestamp}
- Component: {alert.component}
- Severity: {alert.severity.value}
- Message: {alert.message}

Additional Details:
{json.dumps(alert.details, indent=2)}

This is an automated alert from the IoT Data Integration Pipeline.
            """
            
            msg = MimeMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = subject
            msg.attach(MimeText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            self.logger.info(f"Alert email sent: {alert.id}")
            
        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")


class DashboardGenerator:
    """Generates monitoring dashboard data"""
    
    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics = metrics_collector
        self.alerts = alert_manager
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """Generate dashboard data"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'system_health': self._get_system_health(),
            'performance_metrics': self._get_performance_metrics(),
            'active_alerts': self._get_alert_summary(),
            'component_status': self._get_component_status()
        }
    
    def _get_system_health(self) -> Dict[str, Any]:
        """Get system health overview"""
        active_alerts = self.alerts.get_active_alerts()
        critical_alerts = [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
        warning_alerts = [a for a in active_alerts if a.severity == AlertSeverity.WARNING]
        
        if critical_alerts:
            status = "CRITICAL"
        elif warning_alerts:
            status = "WARNING"
        elif active_alerts:
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        
        return {
            'status': status,
            'total_alerts': len(active_alerts),
            'critical_alerts': len(critical_alerts),
            'warning_alerts': len(warning_alerts)
        }
    
    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Get key performance metrics"""
        metrics = {}
        
        key_metrics = [
            'messages.per_second',
            'kafka.consumer.lag',
            'errors.per_minute',
            'system.memory.usage_percent',
            'system.cpu.usage_percent'
        ]
        
        for metric_name in key_metrics:
            summary = self.metrics.get_metric_summary(metric_name, window_minutes=15)
            if summary:
                metrics[metric_name] = summary
        
        return metrics
    
    def _get_alert_summary(self) -> List[Dict[str, Any]]:
        """Get active alerts summary"""
        active_alerts = self.alerts.get_active_alerts()
        return [asdict(alert) for alert in active_alerts[-10:]]  # Last 10 alerts
    
    def _get_component_status(self) -> Dict[str, str]:
        """Get component status overview"""
        # This would be enhanced with actual component health checks
        return {
            'kafka': 'HEALTHY',
            'nifi': 'HEALTHY',
            'spark': 'HEALTHY',
            'mqtt_broker': 'HEALTHY'
        }


def main():
    """Example usage"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Monitoring and Alerting System")
    print("=" * 50)
    
    # Initialize components
    metrics = MetricsCollector()
    alerts = AlertManager()
    monitor = SystemMonitor(metrics, alerts)
    tracker = PerformanceTracker(metrics)
    dashboard = DashboardGenerator(metrics, alerts)
    
    # Add console alert handler
    def console_alert_handler(alert: Alert):
        print(f"🚨 ALERT: [{alert.severity.value}] {alert.component} - {alert.message}")
    
    alerts.add_alert_handler(console_alert_handler)
    
    # Simulate some metrics and alerts
    print("\nSimulating metrics and alerts...")
    
    # Record some metrics
    metrics.record_metric('messages.per_second', 150.0, {'source': 'mqtt'})
    metrics.record_metric('kafka.consumer.lag', 1500.0, {'topic': 'iot-data'})
    metrics.record_metric('system.memory.usage_percent', 87.5)
    
    # Start monitoring
    monitor.start_monitoring()
    
    # Wait a bit for monitoring to trigger
    time.sleep(2)
    
    # Generate dashboard
    dashboard_data = dashboard.generate_dashboard_data()
    print("\nDashboard Data:")
    print(json.dumps(dashboard_data, indent=2))
    
    # Stop monitoring
    monitor.stop_monitoring()
    
    print("\nMonitoring system demonstration complete.")
    print("In production, this would:")
    print("1. Continuously monitor all pipeline components")
    print("2. Send email/SMS alerts for critical issues")
    print("3. Provide real-time dashboard")
    print("4. Track performance trends")
    print("5. Enable proactive maintenance")


if __name__ == "__main__":
    main()