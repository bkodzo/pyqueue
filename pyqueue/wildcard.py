"""Wildcard subscription pattern matching."""

import re
from typing import List, Set


class WildcardMatcher:
    """Matches topic patterns with wildcards."""
    
    @staticmethod
    def match(pattern: str, topic: str) -> bool:
        """Match topic against pattern. Supports * and # wildcards.
        
        * matches single level (e.g., events.* matches events.user but not events.user.login)
        # matches multiple levels (e.g., events.# matches events.user.login)
        """
        if pattern == topic:
            return True
        
        if "#" in pattern:
            parts = pattern.split("#")
            if len(parts) != 2:
                return False
            
            prefix = parts[0].rstrip(".")
            if topic.startswith(prefix):
                return True
        
        if "*" in pattern:
            pattern_parts = pattern.split(".")
            topic_parts = topic.split(".")
            
            if len(pattern_parts) != len(topic_parts):
                return False
            
            for p, t in zip(pattern_parts, topic_parts):
                if p != "*" and p != t:
                    return False
            return True
        
        return False
    
    @staticmethod
    def validate_pattern(pattern: str) -> bool:
        """Validate wildcard pattern syntax."""
        if not pattern:
            return False
        
        if "##" in pattern:
            return False
        
        if pattern.count("#") > 1:
            return False
        
        if "#" in pattern and pattern.index("#") != len(pattern) - 1:
            return False
        
        return True

