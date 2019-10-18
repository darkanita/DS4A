#Prueba
import pandas as pd
import geopandas
from shapely.geometry import Point, Polygon
from tqdm import tqdm
import boto3
from io import StringIO 

def upload_data(data,bucket,key,aws_access_key_id,aws_secret_access_key):
    '''
        Upload data to storage S3, aws_acces_key_id and aws_secret_access_key are provided by AWS.
        This function is only if you require save the information in SW. 
    '''

    client = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    obj = client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue(), ACL='public-read')

    return obj

def make_point(latitude, longitude) :
    '''
        Return a point from latitude and longitude. 
    '''
    return Point( (longitude,latitude) )

def search_ntacode(coord,datanta):
    ntacode = None
    for idy in range(len(datanta['geometry'])):
        if coord.within(datanta['geometry'][idy]):
            ntacode= datanta['ntacode'][idy]
            return ntacode
    return ntacode

def get_ntacode(columnlat,columnlong, datanta, dataori):
    '''
        Return dataframe original with the information of NTA Code. 
    '''
    print("Data Origen: "+str(len(dataori[columnlat])))
    print("Data NTA: "+str(len(datanta['geometry'])))

    dataori['coord'] = dataori.apply(lambda x: make_point(x['pickup_latitude'], x['pickup_longitude']), axis=1)
    dataori['ntacode'] = dataori.apply(lambda x: search_ntacode(x['coord'],datanta), axis=1)
    
    return dataori