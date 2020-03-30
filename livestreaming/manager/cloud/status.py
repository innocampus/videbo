from enum import Enum


class DeploymentStatus(Enum):
    REQUESTED = 1
    CREATED = 2
    INITIALIZED = 3
    TEST_FAILED = 4
    OPERATIONAL = 5
    DESTROYED = 6
    ORPHANED = 7