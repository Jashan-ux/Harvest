from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List
import asyncio

from database import supabase
from scraper import MandiDataScraper  # This should be JharkhandMandiScraper
from scheduler import start_scheduler, stop_scheduler
from models import PriceData, City, Commodity

# Lifespan events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the scheduler when the app starts
    start_scheduler()
    yield
    # Stop the scheduler when the app stops
    stop_scheduler()

app = FastAPI(
    title="Mandi Data API", 
    description="API for agricultural commodity prices in Jharkhand", 
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Mandi Data API for Jharkhand", "status": "active"}

@app.get("/mandi/", response_model=List[PriceData])
async def get_all_mandi_data(limit: int = 100, offset: int = 0):
    """Get all mandi data with pagination (latest first)"""
    try:
        response = supabase.table("price_data").select("*").order("date", desc=True).range(offset, offset + limit - 1).execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

@app.get("/mandi/{city_name}", response_model=List[PriceData])
async def get_mandi_data_by_city(city_name: str, limit: int = 100, offset: int = 0):
    """Get mandi data for a specific city (latest first)"""
    try:
        response = supabase.table("price_data").select("*").eq("city", city_name).order("date", desc=True).range(offset, offset + limit - 1).execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data for {city_name}: {str(e)}")

@app.get("/mandi/{city_name}/{commodity_name}", response_model=List[PriceData])
async def get_mandi_data_by_city_and_commodity(city_name: str, commodity_name: str, limit: int = 100, offset: int = 0):
    """Get mandi data for a specific city and commodity (latest first)"""
    try:
        response = supabase.table("price_data").select("*").eq("city", city_name).eq("commodity", commodity_name).order("date", desc=True).range(offset, offset + limit - 1).execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data for {city_name}/{commodity_name}: {str(e)}")

@app.get("/cities", response_model=List[City])
async def get_all_cities():
    """Get all available cities"""
    try:
        response = supabase.table("cities").select("*").execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching cities: {str(e)}")

@app.get("/commodities", response_model=List[Commodity])
async def get_all_commodities():
    """Get all available commodities"""
    try:
        response = supabase.table("commodities").select("*").execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching commodities: {str(e)}")

@app.post("/refresh-data")
async def refresh_data(background_tasks: BackgroundTasks):
    """Trigger manual data refresh from the website"""
    scraper = MandiDataScraper()  # This should be JharkhandMandiScraper
    background_tasks.add_task(scraper.scrape_and_store_data)
    return {"message": "Data refresh started in the background"}

@app.get("/latest-update")
async def get_latest_update():
    """Get the timestamp of the latest data update"""
    try:
        response = supabase.table("price_data").select("updated_at").order("updated_at", desc=True).limit(1).execute()
        if response.data:
            return {"latest_update": response.data[0]["updated_at"]}
        return {"latest_update": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest update: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)