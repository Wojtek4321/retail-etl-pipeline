import json
import random
import os
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#folder data to generate
os.makedirs('data/input', exist_ok=True)

categories = ['Electronics', 'Garden', 'Fashion', 'Sport', 'Books']
payment_methods = ['Credit Card', 'PayPal', 'Transfer', 'Blik', 'Cash on Delivery']
countries = ['Poland', 'Germany', 'United States', 'England', 'France']

orders_list = []

# fucntion to randomly when generatre error in data
def maybe(probability):
    return random.random() < probability


for i in range(1, 51):  
    order_id = i
    price = round(random.uniform(10.0, 2000.0), 2)
    email = f"user{i}@example.com" 
    category = random.choice(categories)
    payment = random.choice(payment_methods)
    country = random.choice(countries)
    
    # date generation last 30 days
    days_ago = random.randint(0, 30)
    order_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    
    #anomalies
    if maybe(0.1):
        price = -100.0

    if maybe(0.05):  
        order_id = None

    if maybe(0.08):
        email = ""

    if maybe(0.07):
        order_date = "02-04-2026"

    if maybe(0.06):
        country = "Mexico"

    if maybe(0.05):
        category = "Unknown"

    if maybe(0.03):
        payment = "Crypto"

    data = {
        "order_id": order_id,
        "order_date": order_date,
        "customer_email": email,
        "country": country,
        "category": category,
        "amount": price,
        "currency": "PLN",
        "payment_method": payment
    }
    orders_list.append(data)

# save to JSON file
file_path = 'data/input/orders_batch.json'
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(orders_list, f, indent=4, ensure_ascii=False)

logger.info(f"Generated file: {file_path}")
