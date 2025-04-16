class HttpRequestError(Exception):
    def __init__(self, message: str, status: int, url: str, method: str, reason: str | None = None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.url = url
        self.method = method
        self.reason = reason
