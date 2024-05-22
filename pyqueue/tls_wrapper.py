"""TLS/SSL wrapper for secure connections."""

import socket
import ssl
from typing import Optional


def wrap_socket(sock: socket.socket, cert_file: Optional[str] = None, 
                key_file: Optional[str] = None, ca_certs: Optional[str] = None,
                server_side: bool = False) -> ssl.SSLSocket:
    """Wrap a socket with TLS/SSL.
    
    Args:
        sock: Socket to wrap
        cert_file: Path to certificate file (for server)
        key_file: Path to private key file (for server)
        ca_certs: Path to CA certificates file (for client verification)
        server_side: True if this is a server socket
    
    Returns:
        SSL-wrapped socket
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH if server_side else ssl.Purpose.SERVER_AUTH)
    
    if server_side:
        if cert_file and key_file:
            context.load_cert_chain(cert_file, key_file)
        else:
            raise ValueError("cert_file and key_file required for server_side=True")
    
    if ca_certs:
        context.load_verify_locations(ca_certs)
        context.verify_mode = ssl.CERT_REQUIRED
    else:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    return context.wrap_socket(sock, server_side=server_side)

