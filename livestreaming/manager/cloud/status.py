from enum import Enum


class DeploymentStatus(Enum):
    REQUESTED = 1
    CREATED = 2
    INITIALIZING = 3
    INITIALIZED = 4
    OPERATIONAL = 5
    CREATION_FAILED = 6
    INIT_FAILED = 7
    TEST_FAILED = 8
    DESTROYED = 9
    ORPHANED = 10
    UNKNOWN = 11

    @staticmethod
    def state_ok(state):
        if (state == DeploymentStatus.REQUESTED or state == DeploymentStatus.CREATED or
                state == DeploymentStatus.INITIALIZING or state == DeploymentStatus.INITIALIZED or
                state == DeploymentStatus.OPERATIONAL):
            return True
        return False

    @staticmethod
    def state_pending(state):
        return (state == DeploymentStatus.REQUESTED or state == DeploymentStatus.CREATED or
                state == DeploymentStatus.INITIALIZING or state == DeploymentStatus.INITIALIZED)


class VmStatus(Enum):
    CREATED = 0
    RUNNING = 1
    INIT = 2
    OFF = 3
    ERROR = 4
    UNKNOWN = 5
