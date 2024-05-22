"""Custom TCP protocol for PyQueue message broker."""

import json


def encode_message(command, payload=None):
    """Encode a protocol message.
    
    Format: <command> <length> <payload>
    """
    if payload is None:
        payload = b""
    elif isinstance(payload, str):
        payload = payload.encode('utf-8')
    elif isinstance(payload, dict):
        payload = json.dumps(payload).encode('utf-8')
    
    length = len(payload)
    message = f"{command} {length}".encode('utf-8') + b" " + payload
    return message


def decode_message(data):
    """Decode a protocol message.
    
    Returns: (command, payload_bytes)
    """
    parts = data.split(b" ", 2)
    if len(parts) < 2:
        return None, None
    
    command = parts[0].decode('utf-8')
    try:
        length = int(parts[1])
    except ValueError:
        return None, None
    
    if len(parts) == 3:
        payload = parts[2]
    else:
        payload = b""
    
    if len(payload) != length:
        return None, None
    
    return command, payload


def read_message(sock, max_size=10 * 1024 * 1024):
    """Read a complete message from socket.
    
    Args:
        sock: Socket to read from
        max_size: Maximum message size in bytes (default: 10MB)
    
    Returns: (command, payload_bytes) or (None, None) on error
    """
    buffer = b""
    
    while True:
        chunk = sock.recv(1024)
        if not chunk:
            return None, None
        
        buffer += chunk
        
        if len(buffer) > max_size:
            return None, None
        
        first_space = buffer.find(b" ")
        if first_space == -1:
            continue
        
        second_space = buffer.find(b" ", first_space + 1)
        if second_space == -1:
            continue
        
        try:
            command = buffer[:first_space].decode('utf-8')
            length_str = buffer[first_space + 1:second_space].decode('utf-8')
            length = int(length_str)
            
            if length > max_size:
                return None, None
            
            total_needed = second_space + 1 + length
            
            if len(buffer) >= total_needed:
                full_message = buffer[:total_needed]
                return decode_message(full_message)
        except (ValueError, IndexError, UnicodeDecodeError):
            continue

