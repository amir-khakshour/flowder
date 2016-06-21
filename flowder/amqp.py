import json


def amqp_message_create(message, publish_key, meta=None):
    msg = {
        "MESSAGE": message,
        "PUBLISH_KEY": publish_key,
    }

    if meta:
        amqp_message_update_meta(msg, meta)

    return msg


def amqp_message_encode(message):
    return json.dumps(dict(message), ensure_ascii=False).encode("UTF-8")


def amqp_message_decode(encoded_message):
    encoded_message = json.loads(encoded_message)
    return encoded_message


def amqp_message_get_publish_key(message):
    return message.get('PUBLISH_KEY', '*')


def amqp_message_get_meta(message):
    return message.get('AMQP_META', dict())


def amqp_message_update_meta(message, meta):
    return message.update({"AMQP_META": meta})


def escape_string(input):
    input = input.replace('%', '\%')
    return input