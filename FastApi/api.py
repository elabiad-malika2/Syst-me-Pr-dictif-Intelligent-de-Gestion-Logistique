from fastapi import FastAPI , WebSocket
import asyncio
import random
import json
from datetime import datetime ,timezone

app=FastAPI()
REGIONS = ["Southeast Asia","South Asia","Oceania","Eastern Asia","Europe","North America","MENA"]
CITIES = ["Bekasi","Bikaner","Townsville","Guangzhou","Manado","Sangli","Seúl","Jabalpur"]
SEGMENTS = ["Consumer","Corporate","Home Office"]
CATEGORIES = ["Sporting Goods","Office Supplies","Electronics","Furniture"]

def gen_record():
    region = random.choice(REGIONS)
    city = random.choice(CITIES)
    days = random.choices([0,1,2,3,4,5,6,7,8,9,10], weights=[1,3,5,8,6,4,2,1,1,1,1])[0]
    sales = round(random.uniform(5, 1000), 2)
    qty = random.randint(1, 10)
    profit_ratio = round(random.uniform(-1.0, 1.0), 3)
    lat = round(random.uniform(-90,90), 6)
    lon = round(random.uniform(-180,180), 6)
    segment = random.choice(SEGMENTS)
    cat = random.choice(CATEGORIES)
    sales_per_customer = round(sales / max(1, random.randint(1,3)), 6)
    return {
        "Days for shipment (scheduled)": days,
        "Order Region": region,
        "Sales": sales,
        "Order Item Quantity": qty,
        "Order Item Profit Ratio": profit_ratio,
        "Order City": city,
        "Latitude": lat,
        "Longitude": lon,
        "Customer Segment": segment,
        "Category Name": cat,
        "Sales per customer": sales_per_customer,
    }

@app.websocket("/websocket")
async def websocket_api(websocket:WebSocket):
    await websocket.accept()
    while True :
        data = gen_record()

        await websocket.send_text(json.dumps(data))
        await asyncio.sleep(1)