class MyClass:
    def __init__(self):
        self.dynamic_methods = {}

    def add_dynamic_method(self, method_name, method):
        """
        Adds a dynamic method to the class instance.

        Args:
            method_name (str): The name of the new method.
            method (callable): The function object to be added as the method.
        """
        self.dynamic_methods[method_name] = method

    def execute_dynamic_method(self, method_name, *args, **kwargs):
        """
        Executes a dynamic method on the class instance.

        Args:
            method_name (str): The name of the dynamic method to execute.
            *args: Positional arguments to pass to the method.
            **kwargs: Keyword arguments to pass to the method.

        Raises:
            AttributeError: If the specified dynamic method does not exist.
        """
        if method_name not in self.dynamic_methods:
            raise AttributeError(f"Dynamic method '{method_name}' does not exist.")

        method = self.dynamic_methods[method_name]
        return method(*args, **kwargs)

# Create an instance of the class
instance = MyClass()

# Add a dynamic method
def say_hello(name):
    print(f"Hello, {name}!")


def get_rainfall(year):
    print("rainfall for ", year)
def add_nums(a, b):
    return a+b
instance.add_dynamic_method("say_hello", say_hello)

# Execute the dynamic method
instance.execute_dynamic_method("say_hello", "Alice")  # Output: Hello, Alice!
instance.add_dynamic_method("rainfall_source", get_rainfall)
instance.execute_dynamic_method("rainfall_source", 2023)
# instance.add_dynamic_method("add", add_nums)
# k = instance.execute_dynamic_method("add", 2,3,)
# print(k)