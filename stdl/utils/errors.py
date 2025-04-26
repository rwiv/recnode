import requests


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class HttpRequestError(HttpError):
    def __init__(self, message: str, status: int, url: str, method: str | None, reason: str | None = None):
        super().__init__(status, message)
        self.message = message
        self.status = status
        self.url = url
        self.method = method
        self.reason = reason

    @staticmethod
    def from_response(message: str, res: requests.Response) -> "HttpRequestError":
        return HttpRequestError(
            message=message,
            status=res.status_code,
            url=res.url,
            method=res.request.method,
            reason=res.reason,
        )
