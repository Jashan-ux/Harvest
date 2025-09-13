import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin
from datetime import datetime
from database import supabase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MandiDataScraper:
    def __init__(self):
        self.base_url = "https://www.mandiman.in"
        self.state_url = "https://www.mandiman.in/state/jharkhand"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.all_data = []

    def get_city_options(self):
        """Extract city options from the dropdown menu"""
        try:
            response = self.session.get(self.state_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            dropdown = soup.find('select', {'id': 'redirectDropdown'})
            if not dropdown:
                logger.error("Dropdown not found")
                return []
            
            options = []
            for option in dropdown.find_all('option'):
                if option.get('value') and not option.get('disabled') and option.text != 'Select Jharkhand Mandi':
                    options.append({
                        'value': option.get('value'),
                        'slug': option.get('data-slug'),
                        'name': option.text.strip()
                    })
            
            logger.info(f"Found {len(options)} cities")
            return options
        except Exception as e:
            logger.error(f"Error fetching city options: {e}")
            return []

    def get_commodity_details(self, commodity_link, base_data):
        """Get historical data for a commodity from its detail page"""
        try:
            logger.info(f"Fetching historical data from: {commodity_link}")
            response = self.session.get(commodity_link)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'id': 'data-table'})
            if not table:
                return [base_data]
            
            historical_data = []
            rows = table.find_all('tr')
            
            for row in rows:
                # Skip header rows and mobile view rows
                if row.find('th') or 'm_view' in row.get('class', []):
                    continue
                
                cells = row.find_all('td')
                if len(cells) >= 6:
                    try:
                        date = cells[0].text.strip()
                        commodity = cells[1].text.strip()
                        variety = cells[2].text.strip()
                        min_price = cells[3].text.strip()
                        max_price = cells[4].text.strip()
                        modal_price = cells[5].text.strip()
                        
                        historical_data.append({
                            'city': base_data['city'],
                            'date': date,
                            'commodity': commodity,
                            'variety': variety,
                            'min_price': min_price,
                            'max_price': max_price,
                            'modal_price': modal_price,
                            'price_range': f"{min_price} - {max_price}" if min_price and max_price else "N/A"
                        })
                    except Exception as e:
                        logger.warning(f"Error processing historical row: {e}")
                        continue
            
            return historical_data if historical_data else [base_data]
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return [base_data]

    def scrape_city_data(self, city):
        """Get commodity data for a specific city"""
        if city['slug']:
            city_url = f"{self.base_url}/mandi/{city['slug']}"
        else:
            city_url = f"{self.base_url}/mandi/{city['value']}/{city['name'].lower().replace(' ', '-')}"
        
        try:
            logger.info(f"Fetching data for {city['name']}...")
            response = self.session.get(city_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'id': 'data-table'})
            if not table:
                logger.warning(f"No data table found for {city['name']}")
                return []
            
            data = []
            # Find all commodity rows
            commodity_rows = table.find_all('tr', class_='m_view')
            
            for row in commodity_rows:
                try:
                    # Extract commodity name and price range from the summary row
                    commodity_div = row.find('div', class_='float-start')
                    price_div = row.find('div', class_='d-inline')
                    button = row.find('button')
                    
                    if not commodity_div or not button:
                        continue
                    
                    commodity_name = commodity_div.text.strip()
                    price_range = price_div.text.strip() if price_div else "N/A - N/A"
                    
                    # Extract the target ID for the detailed row
                    target_id = button.get('data-bs-target', '').replace('#', '')
                    
                    if target_id:
                        # Find the detailed row
                        detail_row = soup.find('tr', id=target_id)
                        if detail_row:
                            detail_data = self.extract_detail_data(detail_row, city, commodity_name, price_range)
                            if detail_data:
                                data.append(detail_data)
                                
                                # Check if there's a commodity link for historical data
                                commodity_cell = detail_row.find_all('td')[1] if len(detail_row.find_all('td')) > 1 else None
                                if commodity_cell:
                                    link = commodity_cell.find('a')
                                    if link:
                                        commodity_link = urljoin(self.base_url, link.get('href'))
                                        historical_data = self.get_commodity_details(commodity_link, detail_data)
                                        data.extend(historical_data)
                
                except Exception as e:
                    logger.error(f"Error processing commodity row: {e}")
                    continue
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {city['name']}: {e}")
            return []

    def extract_detail_data(self, detail_row, city, commodity_name, price_range):
        """Extract data from the detail row"""
        try:
            cells = detail_row.find_all('td')
            
            if len(cells) >= 6:
                date = cells[0].text.strip()
                commodity = cells[1].text.strip() or commodity_name
                variety = cells[2].text.strip()
                min_price = cells[3].text.strip()
                max_price = cells[4].text.strip()
                modal_price = cells[5].text.strip()
                
                # Return basic data
                return {
                    'city': city['name'],
                    'date': date,
                    'commodity': commodity,
                    'variety': variety,
                    'min_price': min_price,
                    'max_price': max_price,
                    'modal_price': modal_price,
                    'price_range': price_range
                }
        except Exception as e:
            logger.error(f"Error extracting detail data: {e}")
        return None

    def store_data_in_supabase(self):
        """Store scraped data in Supabase - overwrite all existing data"""
        if not self.all_data:
            logger.warning("No data to store")
            return
        
        try:
            # First, delete all existing price data
            logger.info("Deleting all existing price data...")
            supabase.table("price_data").delete().neq("id", 0).execute()
            
            # Store cities (if they don't exist)
            cities = set()
            for item in self.all_data:
                cities.add(item['city'])
            
            for city_name in cities:
                # Check if city already exists
                response = supabase.table("cities").select("*").eq("name", city_name).execute()
                if not response.data:
                    supabase.table("cities").insert({
                        "name": city_name,
                        "slug": city_name.lower().replace(" ", "-"),
                        "value": city_name.lower().replace(" ", "-")
                    }).execute()
            
            # Store commodities (if they don't exist)
            commodities = set()
            for item in self.all_data:
                commodities.add((item['commodity'], item.get('variety', '')))
            
            for commodity_name, variety in commodities:
                # Check if commodity already exists
                response = supabase.table("commodities").select("*").eq("name", commodity_name).execute()
                if not response.data:
                    supabase.table("commodities").insert({
                        "name": commodity_name,
                        "variety": variety
                    }).execute()
            
            # Store all price data (insert all records)
            success_count = 0
            batch_size = 50  # Insert in batches to avoid timeouts
            batches = [self.all_data[i:i + batch_size] for i in range(0, len(self.all_data), batch_size)]
            
            for batch in batches:
                try:
                    batch_data = []
                    for item in batch:
                        batch_data.append({
                            "city": item['city'],
                            "date": item['date'],
                            "commodity": item['commodity'],
                            "variety": item.get('variety', ''),
                            "min_price": item.get('min_price', ''),
                            "max_price": item.get('max_price', ''),
                            "modal_price": item.get('modal_price', ''),
                            "price_range": item.get('price_range', '')
                        })
                    
                    result = supabase.table("price_data").insert(batch_data).execute()
                    success_count += len(batch)
                    logger.info(f"Inserted batch of {len(batch)} records")
                    
                except Exception as e:
                    logger.error(f"Error inserting batch: {e}")
                    # Try inserting records one by one if batch fails
                    for item in batch:
                        try:
                            insert_data = {
                                "city": item['city'],
                                "date": item['date'],
                                "commodity": item['commodity'],
                                "variety": item.get('variety', ''),
                                "min_price": item.get('min_price', ''),
                                "max_price": item.get('max_price', ''),
                                "modal_price": item.get('modal_price', ''),
                                "price_range": item.get('price_range', '')
                            }
                            result = supabase.table("price_data").insert(insert_data).execute()
                            success_count += 1
                        except Exception as single_error:
                            logger.error(f"Error inserting single record for {item['commodity']} in {item['city']}: {single_error}")
                            continue
            
            logger.info(f"Successfully stored {success_count} out of {len(self.all_data)} records in Supabase")
            
        except Exception as e:
            logger.error(f"Error storing data in Supabase: {e}")
            import traceback
            traceback.print_exc()

    def scrape_and_store_data(self):
        """Main method to scrape and store data"""
        logger.info("Starting data scraping...")
        
        cities = self.get_city_options()
        
        if not cities:
            logger.error("No cities found to scrape")
            return
        
        logger.info(f"Found {len(cities)} cities to scrape")
        
        self.all_data = []
        for i, city in enumerate(cities, 1):
            logger.info(f"Processing city {i}/{len(cities)}: {city['name']}")
            city_data = self.scrape_city_data(city)
            if city_data:
                self.all_data.extend(city_data)
            time.sleep(1)  # Be polite with requests
        
        # Store data in Supabase
        self.store_data_in_supabase()
        
        logger.info("Data scraping and storage completed")