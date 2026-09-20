 
from dataclasses import dataclass


@dataclass
class Repository:
    id: int
    name: str
    url: str
    
    is_fork: bool
    
    stargazers: int
    watchers: int
    forks: int
    
    primary_language: str
    visibility: str
    
