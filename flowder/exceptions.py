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


class InvalidAMQPMessage(Exception):
    def __repr__(self):
        return 'The incoming AMQP message has not a valid format.'


class EncodingError(Exception):
    def __repr__(self):
        return 'can\'t encode given message.'