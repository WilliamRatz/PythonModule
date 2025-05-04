class BaseManager:
    """Base class providing common functionality to all managers."""
    
    def __init__(self) -> None:
        """
        Initialization the BaseManager
        """
        # Initiate the default status value.
        self._status = "CREATED"
        
    def log(self, message) -> None:
        """
        The basic logging functionality.
        
        :param message: Message to log in consol.
        """

        # Print the message including the current status of the manager.
        print(f"[{self.__class__.__name__} : {self.status}]: {message}")
        
    def set_status(self, status) -> None:
        """
        Changing the status of the class.
        """
        self._status = status
        
    @property
    def status(self) -> str:
        return self._status