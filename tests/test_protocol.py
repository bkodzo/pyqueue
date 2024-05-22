"""Tests for the protocol module."""

import pytest
from pyqueue.protocol import encode_message, decode_message


class TestProtocol:
    """Tests for protocol encoding/decoding."""
    
    def test_encode_message_no_payload(self):
        """Test encoding a message with no payload."""
        message = encode_message("PING")
        assert message == b"PING 0 "
    
    def test_encode_message_with_string_payload(self):
        """Test encoding a message with a string payload."""
        message = encode_message("PUSH", "hello")
        assert message == b"PUSH 5 hello"
    
    def test_encode_message_with_bytes_payload(self):
        """Test encoding a message with a bytes payload."""
        message = encode_message("PUSH", b"hello")
        assert message == b"PUSH 5 hello"
    
    def test_decode_message_no_payload(self):
        """Test decoding a message with no payload."""
        cmd, payload = decode_message(b"PING 0 ")
        assert cmd == "PING"
        assert payload == b""
    
    def test_decode_message_with_payload(self):
        """Test decoding a message with a payload."""
        cmd, payload = decode_message(b"PUSH 5 hello")
        assert cmd == "PUSH"
        assert payload == b"hello"
    
    def test_decode_message_invalid(self):
        """Test decoding an invalid message."""
        cmd, payload = decode_message(b"INVALID")
        assert cmd is None
        assert payload is None
    
    def test_decode_message_length_mismatch(self):
        """Test decoding a message with incorrect length."""
        cmd, payload = decode_message(b"PUSH 10 hello")
        assert cmd is None
        assert payload is None
    
    def test_roundtrip(self):
        """Test encoding then decoding returns original data."""
        original_payload = "test message with unicode: café"
        message = encode_message("TEST", original_payload)
        cmd, payload = decode_message(message)
        assert cmd == "TEST"
        assert payload.decode('utf-8') == original_payload
