"""Tests for the deduplication module."""

import pytest
import time
from pyqueue.deduplication import DeduplicationStore


class TestDeduplicationStore:
    """Tests for DeduplicationStore."""
    
    def test_new_key_returns_false(self):
        """Test that a new key is not a duplicate."""
        store = DeduplicationStore(ttl_seconds=60)
        assert store.is_duplicate("key1", time.time()) is False
    
    def test_seen_key_returns_true(self):
        """Test that a marked key is a duplicate."""
        store = DeduplicationStore(ttl_seconds=60)
        current = time.time()
        store.mark_seen("key1", current)  # Mark as seen
        assert store.is_duplicate("key1", current) is True  # Now it's a duplicate
    
    def test_different_keys_not_duplicates(self):
        """Test that different keys are not duplicates of each other."""
        store = DeduplicationStore(ttl_seconds=60)
        current = time.time()
        store.mark_seen("key1", current)
        assert store.is_duplicate("key2", current) is False
    
    def test_expired_key_not_duplicate(self):
        """Test that an expired key is not a duplicate."""
        store = DeduplicationStore(ttl_seconds=0.01)  # 10ms TTL
        start = time.time()
        store.mark_seen("key1", start)
        time.sleep(0.02)  # Wait for expiry
        assert store.is_duplicate("key1", time.time()) is False
    
    def test_cleanup_removes_expired(self):
        """Test that cleanup removes expired keys."""
        store = DeduplicationStore(ttl_seconds=0.01)
        start = time.time()
        store.mark_seen("key1", start)
        store.mark_seen("key2", start)
        time.sleep(0.02)
        current = time.time()
        # Cleanup happens automatically in is_duplicate
        assert store.is_duplicate("key1", current) is False
        assert store.is_duplicate("key2", current) is False
    
    def test_none_key_not_duplicate(self):
        """Test that None key is not a duplicate."""
        store = DeduplicationStore(ttl_seconds=60)
        current = time.time()
        assert store.is_duplicate(None, current) is False
        assert store.is_duplicate(None, current) is False  # Still not duplicate
    
    def test_marked_empty_key_is_duplicate(self):
        """Test that marked empty string key is a duplicate."""
        store = DeduplicationStore(ttl_seconds=60)
        current = time.time()
        assert store.is_duplicate("", current) is False
        store.mark_seen("", current)
        assert store.is_duplicate("", current) is True
    
    def test_generate_key_with_idempotency(self):
        """Test key generation with idempotency key."""
        store = DeduplicationStore(ttl_seconds=60)
        key = store.generate_key("payload", "my-key")
        assert key == "idempotency:my-key"
    
    def test_generate_key_from_content(self):
        """Test key generation from content hash."""
        store = DeduplicationStore(ttl_seconds=60)
        key = store.generate_key("payload")
        assert key.startswith("content:")
        assert len(key) > 10  # Should have hash

