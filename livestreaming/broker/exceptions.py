class InsufficientDataException(Exception):
    def __init__(self, missing: list):
        super("Field" + ("s " if len(missing) > 1 else " ") + ", ".join(missing) + " missing at creation")


class NodeAlreadyRegisteredException(Exception):
    def __init__(self, node_id: str):
        super(f"Node <{node_id}> already registered")


class NodeNotFoundException(Exception):
    def __init__(self, node_id: int):
        super(f"Node <{node_id}> does not exist in the pool.")


class MissingLinkException(Exception):
    def __init__(self, node_id: int, encoder_name: str):
        super(f"No connection between <{encoder_name}> and <{node_id}>")


class ContentNodesEmptyException(Exception):
    def __init__(self, encoder_name: str):
        super(f"Encoder Node <{encoder_name}> has no available connection to any content node")
