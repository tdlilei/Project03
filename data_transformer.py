import json
import boto3
import random
import os
import subprocess
import sys
import yfinance 



kinesis = boto3.client("kinesis", "us-west-1")
def getReferrer():
   
    ST = '2021-11-30'
    EN = '2021-12-01'
    Interval = '5m'
    Tieckers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
    for ticker in Tieckers:
        tickerup=  yfinance.Ticker(ticker)
        
        dl = tickerup.history(start=ST, end=EN, interval=Interval)
        for index, rows in dl.iterrows():
            as_jsonstr = json.dumps({
                "high": rows.High, 
                "low": rows.Low, 
                "ts": str(index), 
                "name": ticker
                })+"\n"
                
            out = kinesis.put_record(StreamName="sta9760f2021stream1",Data=as_jsonstr,PartitionKey="partitionkey")
#            print(out)
    return as_jsonstr
     
def lambda_handler(event,context):
    data = getReferrer()
    

    

    return{
        'statusCode': 200,
        'body': data
    }