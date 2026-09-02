def get_hostname():
    """Return the system's hostname."""
    import platform
    return platform.node()

print(get_hostname())