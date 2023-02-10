"""
Mongo Magic Library
~~~~~~~~~~~~~~~~~~
A Python library to interact with MongoDB databases. 
It simplifies the process of CRUD operations.

It is intended to be used in situations where you want to use a simple
object-oriented approach to interacting with MongoDB. It is not intended
to be used in situations where you need to do complex queries or
manipulate the database in any way.

Basic Usage:
~~~~~~~~~~~~
from mongo_magic import init_db, create_collection_class

# Initialize the database connection
# Another option is to use the MONGO_DB_CONNECTION_STRING and MONGO_DB_NAME environment variables
init_db('mongodb://username:password@host:port', 'database_name')

# Create a collection class
User = create_collection_class('User', 'users')

# Create a user object using a dictionary
user = User({
    'first_name': 'Alice',
    'last_name': 'Smith',
    'email': 'alice@email.com'
})

# Create a user object using keyword arguments
user = User(
    first_name='Alice',
    last_name='Smith',
    email='alice@email.com'
)

# Save the object to the database
user.save()

# Search for all users with this first name and return the first hit
# or None if no documents are found
user = User.find(first_name='Alice').first_or_none()
if user:
    # Change the first name
    user.first_name = 'Bob'
    # and save it
    user.save()


:copyright: (c) 2023 by Joakim Wassberg.
:license: MIT License, see LICENSE for more details.
:version: 0.04
"""
from copy import copy
from typing import Union, Self, Callable
import os
import time
import bson
from functools import wraps
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from mongo_magic.mm_exceptions import MongoDBConnectionError, MongoDBCollectionError, MongoFieldError, MondgoDBInvalidDocumentError


class ResultList(list):
    """
    Extends the list class with methods to retrieve the first or last value, or None if the list is empty
    This class is used as a return value for returned documents
    """
    def first_or_none(self):
        """
        Return the first value or None if list is empty
        :return: First list element or None
        """
        return self[0] if len(self) > 0 else None

    def last_or_none(self):
        """
       Return the last value or None if list is empty
       :return: Last list element or None
       """
        return self[-1] if len(self) > 0 else None


class DataBase:
    # The __db object needs to be initialized before the library can be used
    _db = None
    _models = {}


class Document(dict, DataBase):
    """
    This class acts as the base class for collection classes. Each instance of the subclasses
    will represent a single document
    """
    collection = None

    def __init__(self, *args, **kwargs):
        super().__init__()
        # If _id is not present we add the _id attribute
        if '_id' not in kwargs:
            self._id = None

        # Handle dict argument
        if len(args) > 0 and isinstance(args[0], dict):
            d = copy(args[0])
        else:
            d = copy(kwargs)

        # We need to check if there is an embedded document in this document.
        # If so, we will convert it into a dict
        for k, v in d.items():
            if isinstance(v, Document):
                # TODO: In the __init__ method, you are converting embedded documents to a dictionary, but you are not checking if the nested object is another instance of the Document class. This might cause recursion if the nested object also has an embedded object.
                d[k] = v.__dict__

        # Update the object
        self.__dict__.update(d)

    def __repr__(self):
        return '\n'.join(f'{k} = {v}' for k, v in self.__dict__.items())

    def _get_auto_id(self, sequence_name: str, increment: int = 2) -> int:
        """
        Gives you an auto increment field in mongodb
        Works with a collection in your mongodb that needs to have the name
        counters.
        Each document needs to in the form:
        {
            "_id": "sequence_name",
            "sequence_value": 0
        }

        The _id needs to be a unique value per sequence you need to work with, defined as a string.
        The sequence_value is the starting value for the auto increment

        :param sequence_name: str - The name of the sequence to use (mathing the _id)
        :param increment: int - Optional, how much to increment the value each time. Default value is 2
        :return: int - The next value from the auto increment
        """
        if 'counters' not in self._db.list_collection_names():
            raise MongoDBCollectionError('To use an auto increment field you need a collection called counters.')
        
        updated_record = self._db.counters.find_one_and_update(
            filter={"_id": sequence_name},
            upsert=True,
            update={"$inc": {"sequence_value": increment}},
            return_document=True
        )
        return updated_record['sequence_value']

    def save(self, auto_field: Union[str, None] = None, auto_key: Union[str, None] = None) -> Self:
        """
        Saves the current object to the database
        :param auto_field: str | None, if using auto increment key, the name of the key field
        :param auto_key: str | None, name of the key used in counters collection for this auto increment 
        :return: The saved object
        """
        if auto_field:
            if not auto_key:
                raise MongoFieldError('To use auto field, an auto key must be provided')
            self.__dict__[auto_field] = self._get_auto_id(auto_key)

        if self.collection is None:
            raise MongoDBCollectionError('The collection does not exist')

        # If _id is None, this is a new document
        try:
            if self._id is None:
                del (self.__dict__['_id'])
                res = self.collection.insert_one(self.__dict__)
                self._id = res.inserted_id
                return self
            else:
                return self.collection.replace_one({'_id': self._id}, self.__dict__)
        except bson.errors.InvalidDocument as e:
            raise MondgoDBInvalidDocumentError(e)

    def delete_field(self, field: str) -> None:
        """
        Removes a field from this document
        :param field: str, the field to remove
        :return: None
        """
        if field in self.__dict__:
            self.collection.update_one({'_id': self._id}, {"$unset": {field: ""}})
        else:
            raise MongoFieldError(f'{field} does not exist')

    @classmethod
    def get_by_id(cls, _id: str) -> Union[Self, None]:
        """
        Get a document by its _id
        :param _id: str, the id of the document
        :return: The retrieved document or None
        """
        try:
            return cls(cls.collection.find_one({'_id': bson.ObjectId(_id)}))
        except bson.errors.InvalidId:
            return None

    @classmethod
    def insert_many(cls, items: list[dict]) -> None:
        """
        Inserts a list of dictionaries into the databse
        :param items: list of dict, items to insert
        :return: None
        """
        for item in items:
            cls(item).save()

    @classmethod
    def all(cls) -> ResultList:
        """
        Retrieve all documents from the collection
        :return: ResultList of documents
        """
        return ResultList([cls(**item) for item in cls.collection.find({})])

    @classmethod
    def find(cls, **kwargs) -> ResultList:
        """
        Find a document that matches the keywords
        :param kwargs: keyword arguments or dict to match
        :return: ResultList
        """
        if len(kwargs) == 1 and isinstance(kwargs.get(list(kwargs.keys())[0]), dict):
            d = copy(kwargs.get(list(kwargs.keys())[0]))
        else:
            d = copy(kwargs)
        return ResultList(cls(item) for item in cls.collection.find(d))

    @classmethod
    def find_in(cls, field: str, values: list[any]) -> ResultList:
        """
        Find a document that matches the keywords
        :param field: str, the field to search in
        :param values: list, the values to search for
        :return: ResultList
        """
        return ResultList(cls(item) for item in cls.collection.find({field: {"$in": values}}))

    @classmethod
    def delete(cls, **kwargs) -> None:
        """
        Delete the document that matches the keywords
        :param kwargs: keyword arguments or dict to match
        :return: None
        """
        if len(kwargs) == 1 and isinstance(kwargs.get(list(kwargs.keys())[0]), dict):
            d = copy(kwargs.get(list(kwargs.keys())[0]))
        else:
            d = copy(kwargs)
        cls.collection.delete_many(d)

    @classmethod
    def document_count(cls) -> int:
        """
        Returns the total number of documents in the collection
        :return: int
        """
        return cls.collection.count_documents({})


# *******************
# Helper functions
# *******************
def mongo_check_and_connect(func: Callable) -> Callable:
    """
    Decorator to check if the database is connected and connect if not
    :param func: The function to decorate
    :return: Callable
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if DataBase._db is None:
            if os.environ.get('MONGO_DB_CONNECTION_STRING') and os.environ.get('MONGO_DB_NAME'):
                init_db(os.environ.get('MONGO_DB_CONNECTION_STRING'), os.environ.get('MONGO_DB_NAME'))
            else:
                msg = 'init_db function must be called before creation of collection classes.\n' 
                msg += 'Another option is to set the MONGO_DB_CONNECTION_STRING and MONGO_DB_NAME environment variables'
                raise MongoDBConnectionError(msg)
        return func(*args, **kwargs)
    return wrapper


@mongo_check_and_connect
def create_collection_class(class_name: Union[str, None], collection_name: Union[str, None] = None):
    """
    Factory function for creations of collection classes
    :param class_name: str, name of collection class
    :param collection_name: str or None, name of collection in database. If None, the class name will be used
    :return: The newly created collection class
    """
    if collection_name is None:
        collection_name = class_name

    collection_class = type(class_name, (Document, ), {
        'collection': DataBase._db[collection_name]
    })
    return collection_class


@mongo_check_and_connect
def register_model(cls: type, collection_name:  Union[str, None] = None) -> type:
    """
    Register a model class with the database
    :param cls: type, the class to register
    :param collection_name: str or None, name of collection in database. If None, the class name will be used
    :return: type, The registered class
    """
    cls = add_base_class(cls, Document)
    if not hasattr(cls, '__slots__'):
        cls.__slots__ = []
    class_vars = {k: v for k, v in cls.__annotations__.items()}
    cls.__slots__ += list(class_vars.keys())
    for k, v in class_vars.items():
        setattr(cls, k, v)
    if collection_name is None:
        collection_name = cls.__name__.lower()

    cls.collection = DataBase._db[collection_name]
    return cls


def add_base_class(cls, base_class: type) -> type:
    """
    Helper function to add a base class to a collection class
    :param cls: The collection class
    :param base_class: The base class to add
    :return: type, the modified class
    """
    return type(cls.__name__, (base_class, cls, object), dict(cls.__dict__))


def add_collection_method(cls, method: Callable) -> None:
    """
    Helper function to add methods to a collection class.
    Usage:
    def method(self):
        print(self.name)

    user = create_collection_class('User')
    add_collection_method(User, method)
    user.method()
    :param cls: The collection class
    :param method: The method to add to the class
    :return: None
    """
    setattr(cls, method.__name__, method)


def init_db(connection_str: str, database: str, retries: int = 3, retry_delay: int = 2) -> None:
    """
    Function to initialize database connection. Must be called before any use of the library
    :param connection_str: str, the database connection string
    :param database: str, the name of the database to use
    :param retries: int, the number of times to retry connection, defaults to 3
    :param retry_delay: int, the delay between retries, defeults to 2 seconds
    :return: None
    """
    for i in range(retries):
        try:
            client = MongoClient(connection_str)
            client.server_info()
            break
        except ServerSelectionTimeoutError as e:
            if i == retries - 1:
                raise MongoDBConnectionError("Could not connect to database") from e
            else:
                time.sleep(retry_delay ** i)
    DataBase._db = client[database]
