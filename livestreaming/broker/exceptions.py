class InsufficientDataException(Exception):
    def __init__(self, missing: list):
        super("Field" + ("s " if len(missing) > 1 else " ") + ", ".join(missing) + " missing at creation")


class NodeAlreadyRegisteredException(Exception):
    def __init__(self, node_name: str):
        super("Node <%s> already registered".format(node_name))


class NodeNotFoundException(Exception):
    def __init__(self, node_name: str):
        super("Node <%s> does not exist in the pool.".format(node_name))


class MissingLinkException(Exception):
    def __init__(self, node_name: str, encoder_name: str):
        super("No connection between <%s> and <%s>".format(encoder_name, node_name))


class ContentNodesEmptyException(Exception):
    def __init__(self, encoder_name: str):
        super("Encoder Node <%s> has no available connection to any content node".format(encoder_name))
