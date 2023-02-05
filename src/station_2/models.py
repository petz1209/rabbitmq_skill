from pydantic import BaseModel
from typing import List


class Product(BaseModel):
    name: str

class Order(BaseModel):
    order_id: str
    tries: int = 0
    products: List[Product]
