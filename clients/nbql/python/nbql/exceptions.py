class NBQLError(Exception):
    """Base exception for the nbql library."""
    pass

class ConnectionError(NBQLError):
    """Raised for connection-related errors."""
    pass

class APIError(NBQLError):
    """Raised for errors returned by the NBQL API."""
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code

    def __str__(self):
        if self.status_code:
            return f"API Error {self.status_code}: {self.args[0]}"
        return f"API Error: {self.args[0]}"

