#!/usr/bin/env python3
"""
IoT/IIoT Protocol Integration Suite

Implements integration for multiple IoT protocols:
- MQTT (Message Queuing Telemetry Transport)
- SNMP (Simple Network Management Protocol)
- CoAP (Constrained Application Protocol)
- TCP Raw Socket Communication
- WebSocket Real-time Communication

This module provides a unified interface for collecting data from various IoT devices
and industrial systems, formatting it for downstream processing.
"""

import asyncio
import json
import logging
import socket
import ssl
import struct
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import threading
import queue

# Note: These imports would be installed via pip in a real implementation
# pip install paho-mqtt aiocoap pysnmp websockets

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    print("MQTT not available: pip install paho-mqtt")

try:
    from pysnmp.hlapi import *
    SNMP_AVAILABLE = True
except ImportError:
    SNMP_AVAILABLE = False
    print("SNMP not available: pip install pysnmp")

try:
    import aiocoap
    from aiocoap import *
    COAP_AVAILABLE = True
except ImportError:
    COAP_AVAILABLE = False
    print("CoAP not available: pip install aiocoap")

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    print("WebSockets not available: pip install websockets")


@dataclass
class IoTMessage:
    """Standardized IoT message format"""
    timestamp: str
    device_id: str
    protocol: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = None
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


class IoTProtocolHandler(ABC):
    """Abstract base class for IoT protocol handlers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.message_queue = queue.Queue()
        self.is_running = False
        self.callbacks: List[Callable[[IoTMessage], None]] = []
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the IoT service/device"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the IoT service/device"""
        pass
    
    @abstractmethod
    async def start_listening(self) -> None:
        """Start listening for messages"""
        pass
    
    def add_callback(self, callback: Callable[[IoTMessage], None]):
        """Add message callback"""
        self.callbacks.append(callback)
    
    def _notify_callbacks(self, message: IoTMessage):
        """Notify all registered callbacks"""
        for callback in self.callbacks:
            try:
                callback(message)
            except Exception as e:
                self.logger.error(f"Callback error: {e}")


class MQTTHandler(IoTProtocolHandler):
    """MQTT Protocol Handler for IoT device communication"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = None
        self.topics = config.get('topics', ['sensors/+/data'])
        self.broker_host = config.get('host', 'localhost')
        self.broker_port = config.get('port', 1883)
        self.username = config.get('username')
        self.password = config.get('password')
        self.use_ssl = config.get('ssl', False)
    
    async def connect(self) -> bool:
        """Connect to MQTT broker"""
        if not MQTT_AVAILABLE:
            self.logger.error("MQTT library not available")
            return False
        
        try:
            self.client = mqtt.Client(client_id=f"iot_integration_{int(time.time())}")
            
            # Set up SSL if required
            if self.use_ssl:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                self.client.tls_set_context(context)
            
            # Set credentials if provided
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
            
            # Set up callbacks
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect
            
            # Connect to broker
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            self.logger.info(f"Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
            return True
            
        except Exception as e:
            self.logger.error(f"MQTT connection failed: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.logger.info("Disconnected from MQTT broker")
    
    async def start_listening(self) -> None:
        """Start listening for MQTT messages"""
        self.is_running = True
        self.logger.info("Started MQTT message listening")
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            self.logger.info("MQTT connection successful")
            # Subscribe to topics
            for topic in self.topics:
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"MQTT connection failed with code {rc}")
    
    def _on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            # Parse message
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            # Try to parse as JSON
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                data = {"raw_message": payload}
            
            # Extract device ID from topic (assuming format: sensors/{device_id}/data)
            topic_parts = topic.split('/')
            device_id = topic_parts[1] if len(topic_parts) > 1 else "unknown"
            
            # Create standardized message
            message = IoTMessage(
                timestamp=datetime.utcnow().isoformat(),
                device_id=device_id,
                protocol="MQTT",
                data=data,
                metadata={
                    "topic": topic,
                    "qos": msg.qos,
                    "retain": msg.retain
                }
            )
            
            # Notify callbacks
            self._notify_callbacks(message)
            
        except Exception as e:
            self.logger.error(f"Error processing MQTT message: {e}")
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback"""
        self.logger.info("MQTT client disconnected")


class SNMPHandler(IoTProtocolHandler):
    """SNMP Protocol Handler for network device monitoring"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 161)
        self.community = config.get('community', 'public')
        self.oids = config.get('oids', ['1.3.6.1.2.1.1.1.0'])  # System description OID
        self.polling_interval = config.get('polling_interval', 30)
        self.device_id = config.get('device_id', f"snmp_{self.host}")
    
    async def connect(self) -> bool:
        """Validate SNMP connection"""
        if not SNMP_AVAILABLE:
            self.logger.error("SNMP library not available")
            return False
        
        try:
            # Test connection with a simple get
            for (errorIndication, errorStatus, errorIndex, varBinds) in getCmd(
                SnmpEngine(),
                CommunityData(self.community),
                UdpTransportTarget((self.host, self.port)),
                ContextData(),
                ObjectType(ObjectIdentity('1.3.6.1.2.1.1.1.0')),  # System description
                lexicographicMode=False,
                ignoreNonIncreasingOid=False,
                maxRows=1
            ):
                if errorIndication:
                    self.logger.error(f"SNMP connection failed: {errorIndication}")
                    return False
                elif errorStatus:
                    self.logger.error(f"SNMP error: {errorStatus.prettyPrint()}")
                    return False
                else:
                    self.logger.info(f"SNMP connection successful to {self.host}")
                    return True
        except Exception as e:
            self.logger.error(f"SNMP connection error: {e}")
            return False
    
    async def disconnect(self) -> None:
        """SNMP disconnect (no persistent connection)"""
        self.is_running = False
        self.logger.info("SNMP polling stopped")
    
    async def start_listening(self) -> None:
        """Start SNMP polling"""
        self.is_running = True
        self.logger.info("Started SNMP polling")
        
        # Start polling in background
        threading.Thread(target=self._poll_loop, daemon=True).start()
    
    def _poll_loop(self):
        """SNMP polling loop"""
        while self.is_running:
            try:
                self._poll_snmp_data()
                time.sleep(self.polling_interval)
            except Exception as e:
                self.logger.error(f"SNMP polling error: {e}")
                time.sleep(5)  # Wait before retry
    
    def _poll_snmp_data(self):
        """Poll SNMP data from device"""
        try:
            data = {}
            
            for oid in self.oids:
                for (errorIndication, errorStatus, errorIndex, varBinds) in getCmd(
                    SnmpEngine(),
                    CommunityData(self.community),
                    UdpTransportTarget((self.host, self.port)),
                    ContextData(),
                    ObjectType(ObjectIdentity(oid)),
                    lexicographicMode=False,
                    ignoreNonIncreasingOid=False,
                    maxRows=1
                ):
                    if errorIndication:
                        self.logger.error(f"SNMP error for OID {oid}: {errorIndication}")
                        continue
                    elif errorStatus:
                        self.logger.error(f"SNMP error for OID {oid}: {errorStatus.prettyPrint()}")
                        continue
                    else:
                        for varBind in varBinds:
                            oid_name = str(varBind[0])
                            value = str(varBind[1])
                            data[oid_name] = value
            
            # Create standardized message
            message = IoTMessage(
                timestamp=datetime.utcnow().isoformat(),
                device_id=self.device_id,
                protocol="SNMP",
                data=data,
                metadata={
                    "host": self.host,
                    "port": self.port,
                    "community": self.community
                }
            )
            
            # Notify callbacks
            self._notify_callbacks(message)
            
        except Exception as e:
            self.logger.error(f"Error polling SNMP data: {e}")


class CoAPHandler(IoTProtocolHandler):
    """CoAP Protocol Handler for constrained devices"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5683)
        self.resources = config.get('resources', ['/sensors/temperature'])
        self.polling_interval = config.get('polling_interval', 30)
        self.device_id = config.get('device_id', f"coap_{self.host}")
        self.context = None
    
    async def connect(self) -> bool:
        """Initialize CoAP context"""
        if not COAP_AVAILABLE:
            self.logger.error("CoAP library not available")
            return False
        
        try:
            self.context = await aiocoap.Context.create_client_context()
            self.logger.info(f"CoAP context created for {self.host}:{self.port}")
            return True
        except Exception as e:
            self.logger.error(f"CoAP connection failed: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect CoAP context"""
        if self.context:
            await self.context.shutdown()
            self.logger.info("CoAP context shutdown")
    
    async def start_listening(self) -> None:
        """Start CoAP resource polling"""
        self.is_running = True
        self.logger.info("Started CoAP resource polling")
        
        # Start polling loop
        asyncio.create_task(self._poll_loop())
    
    async def _poll_loop(self):
        """CoAP polling loop"""
        while self.is_running:
            try:
                await self._poll_coap_resources()
                await asyncio.sleep(self.polling_interval)
            except Exception as e:
                self.logger.error(f"CoAP polling error: {e}")
                await asyncio.sleep(5)  # Wait before retry
    
    async def _poll_coap_resources(self):
        """Poll CoAP resources"""
        try:
            data = {}
            
            for resource in self.resources:
                uri = f'coap://{self.host}:{self.port}{resource}'
                
                # Create GET request
                request = Message(code=GET, uri=uri)
                
                try:
                    response = await self.context.request(request).response
                    
                    if response.code.is_successful():
                        payload = response.payload.decode('utf-8')
                        
                        # Try to parse as JSON
                        try:
                            resource_data = json.loads(payload)
                        except json.JSONDecodeError:
                            resource_data = {"raw_data": payload}
                        
                        data[resource] = resource_data
                        
                    else:
                        self.logger.warning(f"CoAP resource {resource} returned code: {response.code}")
                        
                except Exception as e:
                    self.logger.error(f"Error accessing CoAP resource {resource}: {e}")
            
            if data:
                # Create standardized message
                message = IoTMessage(
                    timestamp=datetime.utcnow().isoformat(),
                    device_id=self.device_id,
                    protocol="CoAP",
                    data=data,
                    metadata={
                        "host": self.host,
                        "port": self.port,
                        "resources": self.resources
                    }
                )
                
                # Notify callbacks
                self._notify_callbacks(message)
                
        except Exception as e:
            self.logger.error(f"Error polling CoAP resources: {e}")


class TCPHandler(IoTProtocolHandler):
    """TCP Socket Handler for raw TCP communication"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 8080)
        self.device_id = config.get('device_id', f"tcp_{self.host}_{self.port}")
        self.socket = None
        self.buffer_size = config.get('buffer_size', 1024)
        self.timeout = config.get('timeout', 30)
    
    async def connect(self) -> bool:
        """Connect to TCP server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
            self.logger.info(f"TCP connection established to {self.host}:{self.port}")
            return True
        except Exception as e:
            self.logger.error(f"TCP connection failed: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect TCP socket"""
        if self.socket:
            self.socket.close()
            self.logger.info("TCP connection closed")
    
    async def start_listening(self) -> None:
        """Start listening for TCP messages"""
        self.is_running = True
        self.logger.info("Started TCP message listening")
        
        # Start receiving loop
        threading.Thread(target=self._receive_loop, daemon=True).start()
    
    def _receive_loop(self):
        """TCP receive loop"""
        while self.is_running and self.socket:
            try:
                data = self.socket.recv(self.buffer_size)
                if data:
                    self._process_tcp_data(data)
                else:
                    self.logger.warning("TCP connection closed by peer")
                    break
            except socket.timeout:
                continue
            except Exception as e:
                self.logger.error(f"TCP receive error: {e}")
                break
    
    def _process_tcp_data(self, data: bytes):
        """Process received TCP data"""
        try:
            # Try to decode as UTF-8
            try:
                text_data = data.decode('utf-8')
                # Try to parse as JSON
                try:
                    parsed_data = json.loads(text_data)
                except json.JSONDecodeError:
                    parsed_data = {"raw_text": text_data}
            except UnicodeDecodeError:
                # Handle binary data
                parsed_data = {
                    "raw_binary": data.hex(),
                    "size": len(data)
                }
            
            # Create standardized message
            message = IoTMessage(
                timestamp=datetime.utcnow().isoformat(),
                device_id=self.device_id,
                protocol="TCP",
                data=parsed_data,
                metadata={
                    "host": self.host,
                    "port": self.port,
                    "data_size": len(data)
                }
            )
            
            # Notify callbacks
            self._notify_callbacks(message)
            
        except Exception as e:
            self.logger.error(f"Error processing TCP data: {e}")


class WebSocketHandler(IoTProtocolHandler):
    """WebSocket Handler for real-time communication"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.uri = config.get('uri', 'ws://localhost:8080/ws')
        self.device_id = config.get('device_id', 'websocket_client')
        self.websocket = None
        self.headers = config.get('headers', {})
    
    async def connect(self) -> bool:
        """Connect to WebSocket server"""
        if not WEBSOCKETS_AVAILABLE:
            self.logger.error("WebSockets library not available")
            return False
        
        try:
            self.websocket = await websockets.connect(
                self.uri,
                extra_headers=self.headers
            )
            self.logger.info(f"WebSocket connection established to {self.uri}")
            return True
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect WebSocket"""
        if self.websocket:
            await self.websocket.close()
            self.logger.info("WebSocket connection closed")
    
    async def start_listening(self) -> None:
        """Start listening for WebSocket messages"""
        self.is_running = True
        self.logger.info("Started WebSocket message listening")
        
        # Start receiving loop
        asyncio.create_task(self._receive_loop())
    
    async def _receive_loop(self):
        """WebSocket receive loop"""
        try:
            while self.is_running and self.websocket:
                try:
                    message = await self.websocket.recv()
                    self._process_websocket_message(message)
                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("WebSocket connection closed")
                    break
                except Exception as e:
                    self.logger.error(f"WebSocket receive error: {e}")
                    break
        except Exception as e:
            self.logger.error(f"WebSocket receive loop error: {e}")
    
    def _process_websocket_message(self, message: str):
        """Process WebSocket message"""
        try:
            # Try to parse as JSON
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                data = {"raw_message": message}
            
            # Create standardized message
            iot_message = IoTMessage(
                timestamp=datetime.utcnow().isoformat(),
                device_id=self.device_id,
                protocol="WebSocket",
                data=data,
                metadata={
                    "uri": self.uri,
                    "message_size": len(message)
                }
            )
            
            # Notify callbacks
            self._notify_callbacks(iot_message)
            
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")


class IoTIntegrationManager:
    """Central manager for all IoT protocol handlers"""
    
    def __init__(self):
        self.handlers: Dict[str, IoTProtocolHandler] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.message_callbacks: List[Callable[[IoTMessage], None]] = []
    
    def add_handler(self, name: str, handler: IoTProtocolHandler):
        """Add a protocol handler"""
        self.handlers[name] = handler
        handler.add_callback(self._handle_message)
        self.logger.info(f"Added handler: {name}")
    
    def add_message_callback(self, callback: Callable[[IoTMessage], None]):
        """Add global message callback"""
        self.message_callbacks.append(callback)
    
    def _handle_message(self, message: IoTMessage):
        """Handle messages from all protocols"""
        for callback in self.message_callbacks:
            try:
                callback(message)
            except Exception as e:
                self.logger.error(f"Message callback error: {e}")
    
    async def start_all(self):
        """Start all handlers"""
        for name, handler in self.handlers.items():
            try:
                if await handler.connect():
                    await handler.start_listening()
                    self.logger.info(f"Started handler: {name}")
                else:
                    self.logger.error(f"Failed to start handler: {name}")
            except Exception as e:
                self.logger.error(f"Error starting handler {name}: {e}")
    
    async def stop_all(self):
        """Stop all handlers"""
        for name, handler in self.handlers.items():
            try:
                await handler.disconnect()
                self.logger.info(f"Stopped handler: {name}")
            except Exception as e:
                self.logger.error(f"Error stopping handler {name}: {e}")


# Example usage and configuration
def example_usage():
    """Example usage of IoT protocol integration"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create integration manager
    manager = IoTIntegrationManager()
    
    # Add message callback
    def print_message(message: IoTMessage):
        print(f"Received: {message.protocol} from {message.device_id}")
        print(f"Data: {json.dumps(message.data, indent=2)}")
        print("-" * 50)
    
    manager.add_message_callback(print_message)
    
    # Configure handlers
    mqtt_config = {
        'host': 'localhost',
        'port': 1883,
        'topics': ['sensors/+/data', 'actuators/+/status'],
        'username': 'iot_user',
        'password': 'iot_password'
    }
    
    snmp_config = {
        'host': '192.168.1.100',
        'community': 'public',
        'oids': [
            '1.3.6.1.2.1.1.1.0',  # System description
            '1.3.6.1.2.1.1.3.0',  # System uptime
            '1.3.6.1.2.1.2.2.1.10.1'  # Interface bytes in
        ],
        'polling_interval': 30
    }
    
    coap_config = {
        'host': '192.168.1.101',
        'port': 5683,
        'resources': ['/sensors/temperature', '/sensors/humidity'],
        'polling_interval': 60
    }
    
    tcp_config = {
        'host': '192.168.1.102',
        'port': 8080,
        'device_id': 'industrial_plc'
    }
    
    websocket_config = {
        'uri': 'ws://localhost:8080/iot',
        'device_id': 'web_client'
    }
    
    # Add handlers
    manager.add_handler('mqtt', MQTTHandler(mqtt_config))
    manager.add_handler('snmp', SNMPHandler(snmp_config))
    manager.add_handler('coap', CoAPHandler(coap_config))
    manager.add_handler('tcp', TCPHandler(tcp_config))
    manager.add_handler('websocket', WebSocketHandler(websocket_config))
    
    return manager


if __name__ == "__main__":
    """Main execution for testing"""
    
    # Note: This is a demonstration implementation
    # In a real environment, you would:
    # 1. Install required packages: pip install paho-mqtt pysnmp aiocoap websockets
    # 2. Configure actual device endpoints
    # 3. Set up proper error handling and logging
    # 4. Implement data persistence
    # 5. Add authentication and security measures
    
    print("IoT Protocol Integration Suite")
    print("=" * 50)
    print("This is a demonstration implementation.")
    print("To use in production:")
    print("1. Install dependencies: pip install paho-mqtt pysnmp aiocoap websockets")
    print("2. Configure actual device endpoints")
    print("3. Set up proper authentication")
    print("4. Implement data persistence")
    print("5. Add monitoring and alerting")
    
    # Example configuration output
    manager = example_usage()
    print(f"\nConfigured {len(manager.handlers)} handlers:")
    for name in manager.handlers.keys():
        print(f"  - {name}")
    
    print("\nTo run the integration:")
    print("asyncio.run(manager.start_all())")