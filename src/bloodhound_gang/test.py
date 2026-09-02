import socket

def get_hostname():
    """Return the system's hostname."""
    return socket.gethostname()

print(get_hostname())