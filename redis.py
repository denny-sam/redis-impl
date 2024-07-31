import typing
from time import time

class Redis:
    db = {}
    expiry = {}

    def __init__(self):
        self.db = {}
        self.expiry = {}
    
    def set(self, key: str, value: str) -> None:
        self.db[key] = value
    
    def get(self, key: str) -> typing.Optional[str]:
        return self.db.get(key)

    def flush(self):
        self.db = {}

    def incr(self, key: str) -> int:
        self.db[key] = int(self.db.get(key)) + 1
        return self.db[key]
    
    def rpush(self, list_name: str, value: str) -> typing.List:
        if not self.get(list_name):
            self.set(list_name, [])

        self.db[list_name].append(value)
        print(self.get(list_name))
        return self.db[list_name]
    
    def lpush(self, list_name: str, value: str) -> typing.List:
        if not self.get(list_name):
            self.set(list_name, [])
        
        self.db[list_name].insert(0, value)
        print(self.get(list_name))
        return self.db[list_name]
    
    def rpop(self, list_name: str) -> typing.Optional[str]:
        if not self.get(list_name):
            raise ValueError('List is empty')
        return self.db[list_name].pop()
    
    def set_expiry(self, key: str, time_in_secs: int):
        if not self.get(key):
            raise ValueError('Key does not exist')
        
        self.expiry[key] = time() + time_in_secs
    
    def expire_keys(self):
        for key in self.expiry.keys():
            if self.expiry[key] < time():
                del self.db[key]

