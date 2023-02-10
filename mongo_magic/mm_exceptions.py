# *******************
# Library exceptions
# *******************
class MongoException(Exception):
    """
    Base exception class
    """
    pass


class MongoDBConnectionError(MongoException):
    """
    Database initialization exceptions
    """
    pass


class MongoDBCollectionError(MongoException):
    """
    Collection exceptions
    """
    pass

class MongoFieldError(MongoException):
    """
    Field exceptions
    """
    pass

class MongoDBModelExistsError(MongoException):
    """
    Model exceptions
    """
    pass

class MondgoDBInvalidDocumentError(MongoException):
    """
    Document exceptions
    """
    pass