"""Configuration management for PyQueue."""

import os
import json
import yaml
from typing import Dict, Optional, Any


class Config:
    """Configuration manager with file and environment variable support."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config: Dict[str, Any] = {}
        self.config_file = config_file or os.getenv('PYQUEUE_CONFIG', 'pyqueue.yaml')
        self._load_config()
        self._load_env_overrides()
    
    def _load_config(self):
        """Load configuration from file."""
        if not os.path.exists(self.config_file):
            self._set_defaults()
            return
        
        try:
            with open(self.config_file, 'r') as f:
                if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                    self.config = yaml.safe_load(f) or {}
                elif self.config_file.endswith('.json'):
                    self.config = json.load(f)
                else:
                    self._set_defaults()
                    return
        except Exception:
            self._set_defaults()
            return
        
        if not self.config:
            self._set_defaults()
    
    def _set_defaults(self):
        """Set default configuration values."""
        self.config = {
            "broker": {
                "host": "0.0.0.0",
                "port": 5555,
                "visibility_timeout": 10,
                "max_retries": 3,
                "journal_file": "journal.log"
            },
            "security": {
                "enabled": False,
                "api_key": None,
                "jwt_secret": None,
                "tls_enabled": False,
                "tls_cert_file": None,
                "tls_key_file": None
            },
            "queues": {
                "default_max_depth": 10000,
                "default_ttl": None,
                "enable_backpressure": True
            },
            "compression": {
                "enabled": True,
                "algorithm": "gzip",
                "min_size_bytes": 1024
            },
            "monitoring": {
                "prometheus_enabled": True,
                "prometheus_port": 9090,
                "metrics_interval": 10
            },
            "raft": {
                "election_timeout": 5.0,
                "heartbeat_interval": 2.0,
                "snapshot_interval": 3600,
                "snapshot_threshold": 1000
            },
            "rate_limiting": {
                "default_rate": 10.0,
                "default_burst": 20
            }
        }
    
    def _load_env_overrides(self):
        """Override config with environment variables."""
        env_mappings = {
            "BROKER_HOST": ("broker", "host"),
            "BROKER_PORT": ("broker", "port", int),
            "VISIBILITY_TIMEOUT": ("broker", "visibility_timeout", int),
            "MAX_RETRIES": ("broker", "max_retries", int),
            "JOURNAL_FILE": ("broker", "journal_file"),
            "SECURITY_ENABLED": ("security", "enabled", lambda x: x.lower() == "true"),
            "API_KEY": ("security", "api_key"),
            "JWT_SECRET": ("security", "jwt_secret"),
            "TLS_ENABLED": ("security", "tls_enabled", lambda x: x.lower() == "true"),
            "TLS_CERT_FILE": ("security", "tls_cert_file"),
            "TLS_KEY_FILE": ("security", "tls_key_file"),
            "QUEUE_MAX_DEPTH": ("queues", "default_max_depth", int),
            "QUEUE_TTL": ("queues", "default_ttl", int),
            "COMPRESSION_ENABLED": ("compression", "enabled", lambda x: x.lower() == "true"),
            "COMPRESSION_MIN_SIZE": ("compression", "min_size_bytes", int),
            "PROMETHEUS_ENABLED": ("monitoring", "prometheus_enabled", lambda x: x.lower() == "true"),
            "PROMETHEUS_PORT": ("monitoring", "prometheus_port", int)
        }
        
        for env_key, (section, key, *converters) in env_mappings.items():
            value = os.getenv(env_key)
            if value is not None:
                if converters:
                    converter = converters[0]
                    try:
                        value = converter(value)
                    except (ValueError, TypeError):
                        continue
                if section not in self.config:
                    self.config[section] = {}
                self.config[section][key] = value
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(section, {}).get(key, default)
    
    def set(self, section: str, key: str, value: Any):
        """Set configuration value."""
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
    
    def save(self, filename: Optional[str] = None):
        """Save configuration to file."""
        target_file = filename or self.config_file
        with open(target_file, 'w') as f:
            if target_file.endswith('.yaml') or target_file.endswith('.yml'):
                yaml.dump(self.config, f, default_flow_style=False)
            else:
                json.dump(self.config, f, indent=2)
    
    def validate(self) -> tuple[bool, list[str]]:
        """Validate configuration."""
        errors = []
        
        if self.get("broker", "port") < 1 or self.get("broker", "port") > 65535:
            errors.append("Invalid broker port")
        
        if self.get("security", "enabled"):
            if not self.get("security", "api_key") and not self.get("security", "jwt_secret"):
                errors.append("Security enabled but no API key or JWT secret provided")
            
            if self.get("security", "tls_enabled"):
                cert_file = self.get("security", "tls_cert_file")
                key_file = self.get("security", "tls_key_file")
                if not cert_file or not key_file:
                    errors.append("TLS enabled but certificate files not provided")
                elif not os.path.exists(cert_file):
                    errors.append(f"TLS certificate file not found: {cert_file}")
                elif not os.path.exists(key_file):
                    errors.append(f"TLS key file not found: {key_file}")
        
        return len(errors) == 0, errors

