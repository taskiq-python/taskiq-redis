from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError


class TaskIQRedisError(TaskiqError):
    """Base error for all taskiq-redis exceptions."""


class DuplicateExpireTimeSelectedError(ResultBackendError, TaskIQRedisError):
    """Error if two lifetimes are selected."""

    __template__ = "Choose either result_ex_time or result_px_time."


class ExpireTimeMustBeMoreThanZeroError(ResultBackendError, TaskIQRedisError):
    """Error if two lifetimes are less or equal zero."""

    __template__ = (
        "You must select one expire time param and it must be more than zero."
    )


class ResultIsMissingError(TaskIQRedisError, ResultGetError):
    """Error if there is no result when trying to get it."""
