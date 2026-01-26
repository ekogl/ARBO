class TaskAlreadyExistsError(Exception):
    """Raised when trying to initialize a task that already exists in the DB"""
    pass

class TaskNotFoundError(Exception):
    """Raised when trying to fetch a task that does not exist in the DB"""
    pass