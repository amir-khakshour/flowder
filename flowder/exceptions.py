class NoResponseContent(Exception):
    """
    When the response is empty
    """
    pass


class InvalidResponseRetry(Exception):
    """
    When the response is invalid and we want to retry
    """
    pass