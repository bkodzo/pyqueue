"""Authentication and authorization for PyQueue."""

import time
import hmac
import hashlib
import jwt
from typing import Optional, Dict, Set
from .config import Config


class Authenticator:
    """Handles authentication and authorization."""
    
    def __init__(self, config: Config):
        self.config = config
        self.enabled = config.get("security", "enabled", False)
        self.api_key = config.get("security", "api_key")
        self.jwt_secret = config.get("security", "jwt_secret")
        self.permissions: Dict[str, Set[str]] = {}
        self._load_permissions()
    
    def _load_permissions(self):
        """Load permissions from config."""
        perms_config = self.config.get("security", "permissions", {})
        if perms_config:
            for principal, queues in perms_config.items():
                self.permissions[principal] = set(queues)
    
    def authenticate_api_key(self, api_key: str) -> Optional[str]:
        """Authenticate using API key. Returns principal name or None."""
        if not self.enabled:
            return "anonymous"
        
        if not self.api_key:
            return None
        
        if hmac.compare_digest(api_key, self.api_key):
            return "api_key_user"
        
        return None
    
    def authenticate_jwt(self, token: str) -> Optional[Dict]:
        """Authenticate using JWT token. Returns claims or None."""
        if not self.enabled:
            return {"principal": "anonymous"}
        
        if not self.jwt_secret:
            return None
        
        try:
            claims = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            return claims
        except jwt.InvalidTokenError:
            return None
    
    def authorize(self, principal: str, queue: str, operation: str) -> bool:
        """Check if principal is authorized for operation on queue."""
        if not self.enabled:
            return True
        
        if principal not in self.permissions:
            return False
        
        allowed_queues = self.permissions[principal]
        if "*" in allowed_queues:
            return True
        
        if queue in allowed_queues:
            return True
        
        for pattern in allowed_queues:
            if self._match_pattern(pattern, queue):
                return True
        
        return False
    
    def _match_pattern(self, pattern: str, queue: str) -> bool:
        """Match queue name against pattern (supports * and ? wildcards)."""
        if pattern == queue:
            return True
        
        if "*" in pattern:
            import re
            regex = pattern.replace("*", ".*").replace("?", ".")
            return bool(re.match(regex, queue))
        
        return False
    
    def create_jwt_token(self, principal: str, expires_in: int = 3600) -> str:
        """Create a JWT token for a principal."""
        if not self.jwt_secret:
            raise ValueError("JWT secret not configured")
        
        claims = {
            "principal": principal,
            "iat": int(time.time()),
            "exp": int(time.time()) + expires_in
        }
        
        return jwt.encode(claims, self.jwt_secret, algorithm="HS256")

