from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from scraper import MandiDataScraper

# Create a scheduler instance
scheduler = BackgroundScheduler()

def scheduled_scrape():
    """Function to be called by the scheduler"""
    print("Running scheduled data scraping...")
    scraper = MandiDataScraper()
    scraper.scrape_and_store_data()

def start_scheduler():
    """Start the scheduled task"""
    # Schedule the scraper to run daily at 2 AM
    scheduler.add_job(
        scheduled_scrape,
        trigger=CronTrigger(hour=15, minute=7),
        id='daily_scrape',
        name='Scrape mandi data daily at 2 AM',
        replace_existing=True
    )
    
    # Start the scheduler
    if not scheduler.running:
        scheduler.start()
        print("Scheduler started")

def stop_scheduler():
    """Stop the scheduler"""
    if scheduler.running:
        scheduler.shutdown()
        print("Scheduler stopped")