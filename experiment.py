import pandas as pd
from data import *

pd.options.mode.chained_assignment = None 

def main(aws_access_key_id,aws_secret_access_key):
    bucket = 'DS4A'
    # NTA
    NTA = geopandas.read_file('https://ds4adata.s3-sa-east-1.amazonaws.com/Datathon/NTA+map.geojson')
    print("NTA Read: "+str(NTA.shape))
    #UBER 2014
    uber_trips_2014 = pd.read_csv('https://ds4adata.s3-sa-east-1.amazonaws.com/Datathon/uber_trips_2014.csv')
    print(uber_trips_2014.head(1))
    print("UBER 2014 Read: "+str(uber_trips_2014.shape))
    key = 'uber_trips_2014.csv'
    uber_trips_2014['coord']=None
    uber_trips_2014['ntacode']=None
    uber_trips_2014_new = get_ntacode('pickup_latitude','pickup_longitude', NTA, uber_trips_2014)
    uploadObj = upload_data(uber_trips_2014_new,bucket,key,aws_access_key_id,aws_secret_access_key)
    print('Uber 2014 Updated: '+ uploadObj)
    print("UBER 2014 Updated: "+str(uber_trips_2014_new.shape))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Prepare dataset")

    parser.add_argument('--aws_access_key_id', '-aws_id',
        help="aws_access_key_id"
    )

    parser.add_argument('--aws_secret_access_key', '-aws_key',
        help="aws_secret_access_key"
    )
    
    args = parser.parse_args()
    main(args.aws_access_key_id, args.aws_secret_access_key)