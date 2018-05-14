import os
import csv
import json
from datetime import datetime 
from itertools import islice

from pymongo import MongoClient

from utils import chunker


def embed_ride(ride, column_remapping): 
    """[('trip_start_timestamp', '2016-1-13 06:15:00'), ('trip_end_timestamp', '2016-1-13 06:15:00'), 
        ('trip_seconds', '180'), ('trip_miles', '0.4'), 
        ('pickup_census_tract', ''), ('dropoff_census_tract', ''), 
        ('pickup_community_area', '24'), ('dropoff_community_area', '24'), 
        ('fare', '4.50'), ('tips', '0.00'), ('tolls', '0.00'), ('extras', '0.00'), ('trip_total', '4.50'),
        ('payment_type', 'Cash'), 
        ('company', '107'), 
        ('pickup_latitude', '199'), ('pickup_longitude', '510'), 
        ('dropoff_latitude', '199'), ('dropoff_longitude', '510'), ('taxi', {'id': '85'})]"""
    if ride['taxi_id'] != '':
        ride['taxi'] = { 'id': int(ride['taxi_id']) }
        ride['company'] = ride.get('company')
        if ride['company']:
            company_id = ride['company']
            ride['taxi']['company'] = {
                'id': int(company_id),
                'name': column_remapping['company'][company_id]
            } 
        
    del ride['taxi_id']
    del ride['company']

    ride['trip_start_timestamp'] = datetime.strptime(ride['trip_start_timestamp'], "%Y-%m-%d %H:%M:%S") if ride['trip_start_timestamp'] != '' else None
    ride['trip_end_timestamp'] = datetime.strptime(ride['trip_end_timestamp'], "%Y-%m-%d %H:%M:%S") if ride['trip_end_timestamp'] != '' else None

    ride_fare = float(ride['fare']) if ride['fare'] != '' and ride['fare'] != '0.00' else None
    ride_tips = float(ride['tips']) if ride['tips'] != '' and ride['tips'] != '0.00' else None
    ride_tolls = float(ride['tolls']) if ride['tolls'] != '' and ride['tolls'] != '0.00' else None
    ride_extras = float(ride['extras']) if ride['extras'] != '' and ride['extras'] != '0.00' else None
    ride_payment_type = ride['payment_type']
    if ride_fare or ride_tips or ride_tolls or ride_extras or ride_payment_type:
        payment = {
            'fare': ride_fare,
            'tips': ride_tips, 
            'tolls': ride_tolls, 
            'extras': ride_extras, 
            'payment_type': ride_payment_type 
        } 
        payment = {k: v for k, v in payment.items() if v is not None}
        ride['payment'] = payment

    del ride['trip_total']    
    del ride['fare']
    del ride['tips']
    del ride['tolls']
    del ride['extras']
    del ride['payment_type']
    
    ride['trip_seconds'] = int(ride['trip_seconds']) if ride['trip_seconds'] != '' and ride['trip_seconds'] != '0' else None
    ride['trip_miles'] = float(ride['trip_miles']) if ride['trip_miles'] != '' and ride['trip_miles'] != '0' else None

    ride['pickup_latitude'] = ride['pickup_latitude'] if ride['pickup_latitude'] != '' else None 
    ride['pickup_longitude'] = ride['pickup_longitude'] if ride['pickup_longitude'] != '' else None

    ride['dropoff_latitude'] = ride['dropoff_latitude'] if ride['dropoff_latitude'] != '' else None 
    ride['dropoff_longitude'] = ride['dropoff_longitude'] if ride['dropoff_longitude'] != '' else None

    if ride['pickup_latitude'] is None or ride['pickup_longitude'] is None:
        ride['pickup_location'] = None
    else:
        pickup_latitude = float(column_remapping['pickup_latitude'][ride['pickup_latitude']])
        pickup_longitude = float(column_remapping['pickup_longitude'][ride['pickup_longitude']])
        ride["pickup_location"] = { 
            "type": "Point",
            "coordinates": [pickup_longitude, pickup_latitude]
        }
    del ride['pickup_latitude']
    del ride['pickup_longitude']
   
    if ride['dropoff_latitude'] is None or ride['dropoff_longitude'] is None:
        ride['dropoff_location'] = None
    else:
        dropoff_latitude = float(column_remapping['dropoff_latitude'][ride['dropoff_latitude']])
        dropoff_longitude = float(column_remapping['dropoff_longitude'][ride['dropoff_longitude']])
        ride["dropoff_location"] = { 
            "type": "Point",
            "coordinates": [dropoff_longitude, dropoff_latitude]
        }
    del ride['dropoff_latitude']
    del ride['dropoff_longitude']
   
    del ride['pickup_census_tract']
    del ride['dropoff_census_tract']
    del ride['pickup_community_area']
    del ride['dropoff_community_area']

    ride = {k: v for k, v in ride.items() if v is not None}

    return ride

if __name__ == '__main__':
        
    client = MongoClient('localhost', 27017, username='root', password='password')
    db = client["taxiRides"]
    rides_collection = db["rides"]
    
    folder, _ = os.path.split(__file__)
    
    column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))
    
    FILES = ['2016_01']
    for FILE in FILES:
        print(f"Importing {FILE} Rides")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.DictReader(csvfile)

            i = 0 # for progress
            CHUNKER_SIZE = 1000
            rides = islice(rides, 1000)
            for rides_chunk in chunker(rides, CHUNKER_SIZE):
                rides_chunk = [embed_ride(ride, column_remapping) for ride in rides_chunk]
                i += CHUNKER_SIZE
                if (i % 10000 == 0):
                    print(f"Progress: {i}")

                rides_collection.insert_many(rides_chunk)
