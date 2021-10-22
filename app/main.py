from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query
from fastapi.responses import RedirectResponse

from typing import Optional,List
from pydantic import BaseModel
import  pymysql
import os
import logging 
import json
import datetime
import boto3

# fast api
app = FastAPI(title='Matching Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


# pydantic model
class tQuery(BaseModel):
    query_id:str
    timestamp:str
    publisher_id:int
    category:int
    zip_code:str

class Impression(BaseModel):
    query_id:str
    impression_id:str
    timestamp:str
    publisher_id:int
    advertiser_id:int
    advertiser_campaign_id:int
    category:int
    ad_id:int
    zip_code:str
    advertiser_price:float
    publisher_price:float
    position:int

class Click(BaseModel):
    query_id:str
    impression_id:str
    click_id:str
    timestamp:str
    publisher_id:int 
    advertiser_id:int 
    advertiser_campaign_id:int 
    category:int 
    ad_id:int
    zip_code:str
    advertiser_price:float
    publisher_price:float
    position:int


# routes
@app.get("/")
def read_root():
    return {"Service": "tracking"}

@app.post("/tracking/query")

def query(query:tQuery):
    query_dict = query.dict()

    data = {
            "Data":json.dumps({
                "query_id":query.query_id,
                "timestamp":query.timestamp,
                "publisher_id":query.publisher_id,
                "category":query.category,
                "zip_code":query.zip_code
            })+"\n"
        }

    client = boto3.client('firehose',region_name='us-east-2')
    
    
    response = client.put_record(
    DeliveryStreamName='query',
    Record=data
    )
    

    logger.error(response)
    return {"Service": "tracking query"}

@app.post("/tracking/impression")

async def impression(impression:Impression):
    impression_dict = impression.dict()

    data = {
            "Data":json.dumps({
                "query_id":impression.query_id,
                "impression_id":impression.impression_id,
                "timestamp":impression.timestamp,
                "publisher_id":impression.publisher_id,
                "advertiser_id":impression.advertiser_id,
                "advertiser_campaign_id":impression.advertiser_campaign_id,
                "category":impression.category,
                "ad_id":impression.ad_id,
                "zip_code":impression.zip_code,
                "advertiser_price":impression.advertiser_price,
                "publisher_price":impression.publisher_price,
                "position":impression.position
            })+"\n"
        }

    client = boto3.client('firehose',region_name='us-east-2')
    
    response = client.put_record(
    DeliveryStreamName='impression',
    Record=data
    )
    

    return {"Service": "tracking impression"}

@app.post("/tracking/click")
async def click(click:Click):
    click_dict = click.dict()

    data = {
        "Data":json.dumps({
                "query_id":click.query_id,
                "impression_id":click.impression_id,
                "click_id":click.click_id,
                "timestamp":click.timestamp,
                "publisher_id":click.publisher_id,
                "advertiser_id":click.advertiser_id,
                "advertiser_campaign_id":click.advertiser_campaign_id,
                "category":click.category,
                "ad_id":click.ad_id,
                "zip_code":click.zip_code,
                "advertiser_price":click.advertiser_price,
                "publisher_price":click.publisher_price,
                "position":click.position
            })+"\n"
    }

    client = boto3.client('firehose',region_name='us-east-2')

    response = client.put_record(
    DeliveryStreamName='click',
    Record=data
    )
    

    logger.error(response)
    return {"Service": "tracking click"}
