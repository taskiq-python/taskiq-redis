class TaskIQRedisError(Exception):
    """Base error for all taskiq-redis exceptions."""


class DuplicateExpireTimeSelectedError(TaskIQRedisError):
    """Error if two lifetimes are selected."""


class ExpireTimeMustBeMoreThanZeroError(TaskIQRedisError):
    """Error if two lifetimes are less or equal zero."""
