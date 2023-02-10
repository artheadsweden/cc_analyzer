from mongo_magic.mongo_doc import mongo_check_and_connect, Document
import inspect

def preserve_class_attrs(cls: type, NewClass: type) -> type:
    """
    Preserve the class attributes of cls in NewClass
    :param cls: The class to preserve the attributes from
    :param NewClass: The class to preserve the attributes in
    :return: The NewClass with the preserved attributes
    """
    # Keep the original name
    NewClass.__name__ = cls.__name__
    NewClass.__qualname__ = cls.__qualname__
    NewClass.__doc__ = cls.__doc__
    # Check if annotations is present, if it is then update it in NewClass
    if hasattr(cls, '__annotations__'):
        NewClass.__annotations__ = cls.__annotations__
    # Check if __slots__ is present, if it is then update it in NewClass
    if hasattr(cls, '__slots__'):
        NewClass.__slots__ = cls.__slots__
    
    return NewClass

    

def model(collection_name: str) -> callable:
    """
    Decorator to create a model from a class. 
    The class must have type annotations.
    The decorator will create a new class that inherits from Document and the class passed to the decorator.
    This class will have a constructor that will check the types of the arguments passed to it.
    :param collection_name: The name of the collection to use for the model
    :return: A function that will create a new class
    """
    def _model(cls: type):
        @mongo_check_and_connect
        class NewClass(Document, cls):
            def __init__(self, *args, **kwargs):
                for name, type_ in cls.__annotations__.items():
                    if name in kwargs:
                        value = kwargs.pop(name)
                    elif args:
                        value = args[0]
                        args = args[1:]
                    else:
                        value = type_()
                    if not isinstance(value, type_):
                        raise TypeError(f"Expected {name} to be of type {type_}, but got {type(value)}")
                    self.__dict__[name] = value
                self.__dict__['_id'] = None
                super().__init__(*args, **kwargs)
                NewClass.collection = super()._db[collection_name]
                print()

            def __setattr__(self, name, value):
                if not hasattr(self, name):
                    raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")
                super().__setattr__(name, value)

        #NewClass.collection = collection_name
        
        return preserve_class_attrs(cls, NewClass)
    return _model

# @mongo_check_and_connect
# def mongo_model(cls):
#     class_vars = {'self': cls}
#     class_vars.update({k: v for k, v in cls.__annotations__.items()})
#     annotations = list(class_vars.keys())

#     cls.collection = cls._db[cls.__name__.lower() + 's']

#     def __init__(self, **kwargs):
#         for k, v in kwargs.items():
#             if k not in annotations:
#                 raise TypeError(f'Argument {k} is not allowed')
#         for k, v in class_vars.items():
#             if type(kwargs.get(k)) is not v:
#                 raise TypeError(f'Argument {k} must be {v}')
        
#         self.__dict__.update(kwargs)
#         self._id = None
#     __init__.__signature__ = inspect.Signature(parameters=[inspect.Parameter(arg, inspect.Parameter.POSITIONAL_OR_KEYWORD) for arg in class_vars])
#     cls.__init__ = __init__
#     return cls