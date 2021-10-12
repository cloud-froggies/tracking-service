from typing import Optional,List

from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query
from fastapi.responses import RedirectResponse

import  pymysql
import os
import logging 
import json
from pydantic import BaseModel

import datetime
import boto3

# DB_ENDPOINT = os.environ.get('db_endpoint')
# DB_ADMIN_USER = os.environ.get('db_admin_user')
# DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
# DB_NAME = os.environ.get('db_name')

# AWS_ACCESS_KEY_ID =''
# AWS_SECRET_ACCESS_KEY=''

app = FastAPI(title='Matching Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

    

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
    publisher_id:int 
    advertiser_id:int 
    advertiser_campaign_id:int 
    category:int 
    ad_id:int
    zip_code:str
    advertiser_price:float
    publisher_price:float
    position:int

@app.get("/")
def read_root():
    return {"Service": "tracking"}

@app.post("/query")
def query(query:tQuery):
    query_dict = query.dict()

    data = [
        {
            "Data":json.dumps({
                "query_id":query.query_id,
                "timestamp":query.timestamp,
                "publisher_id":query.publisher_id,
                "category":query.category,
                "zip_code":query.zip_code
            }),
        }
    ]
    client = boto3.client('firehose',region_name='us-east-2')
    
    response = client.put_record_batch(DeliveryStreamName="query", Records=data)

    logger.error(response)

    return {"Service": "tracking query"}

@app.post("/impression")
async def impression(impression:Impression):
    impression_dict = impression.dict()

    data = [
        {
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
            }),
        }
    ]
    client = boto3.client('firehose',region_name='us-east-2')
    
    response = client.put_record_batch(DeliveryStreamName="impression", Records=data)

    logger.error(response)


    return {"Service": "tracking impression"}

@app.post("/click")
async def click(click:Click):
    click_dict = click.dict()

    data = [
        {
            "Data":json.dumps({
                "publisher_id":click.publisher_id,
                "advertiser_id":click.advertiser_id,
                "advertiser_campaign_id":click.advertiser_campaign_id,
                "category":click.category,
                "ad_id":click.ad_id,
                "zip_code":click.zip_code,
                "advertiser_price":click.advertiser_price,
                "publisher_price":click.publisher_price,
                "position":click.position
            }),
        }
    ]
    client = boto3.client('firehose',region_name='us-east-2')
    
    response = client.put_record_batch(DeliveryStreamName="click", Records=data)

    logger.error(response)


    return {"Service": "tracking click"}
