class BaseManager:
    """Base class providing common functionality to all managers"""
    
    def __init__(self):
        self._status = "CREATED"
        
    def log(self, message):
        """Basic logging functionality"""
        print(f"[{self.__class__.__name__} : {self.status}]: {message}")
        
    def set_status(self, status):
        """Changing status of class"""
        self._status = status
        
    @property
    def status(self):
        return self._status