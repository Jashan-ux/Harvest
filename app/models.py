from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class City(BaseModel):
    id: int
    name: str
    slug: str
    value: str
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class Commodity(BaseModel):
    id: int
    name: str
    variety: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class PriceData(BaseModel):
    id: int
    city: str
    date: str
    commodity: str
    variety: Optional[str] = None
    min_price: str
    max_price: str
    modal_price: Optional[str] = None
    price_range: Optional[str] = None
    mandi: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True