from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError


class TaskIQRedisError(TaskiqError):
    """Base error for all taskiq-redis exceptions."""


class DuplicateExpireTimeSelectedError(ResultBackendError, TaskIQRedisError):
    """Error if two lifetimes are selected."""


class ExpireTimeMustBeMoreThanZeroError(ResultBackendError, TaskIQRedisError):
    """Error if two lifetimes are less or equal zero."""


class ResultIsMissingError(TaskIQRedisError, ResultGetError):
    """Error if there is no result when trying to get it."""
