import os
import csv
import json
from itertools import islice

import mysql.connector
from mysql.connector import errorcode

from utils import make_connection

connection = make_connection()
cursor = connection.cursor()

add_ride_query = """
    INSERT INTO `rides`
    (`taxi_id`, `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, `trip_miles`, `fare`, `tips`, `tolls`, `extras`, `payment_type`, `start_location`, `end_location`, `company_id`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {0}, {1}, %s)
"""
add_company_query = """
    INSERT INTO `companies`
    (`id`, `name`)
    VALUES (%s, %s)
"""

folder, _ = os.path.split(__file__)

column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))

FILES = ['2016_01']
for FILE in FILES:
    print(f"Importing {FILE} Rides")
    with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
        rides = csv.reader(csvfile)

        header = next(rides) # skip header

        for ride in islice(rides, 1000):
            ride = dict(zip(header, ride))

            # check if company exists inside the companies table
            # if it does not exists create it
            if ride['company'] != '': 
                company_id = ride['company']
                ride['company'] = int(ride['company'])
                # a company is set
                try:
                    cursor.execute(
                        add_company_query,
                        [int(company_id), column_remapping['company'][company_id]]
                    )
                except mysql.connector.Error as err:
                    #print(err)
                    pass
            else:
                ride['company'] = None

            ride['taxi_id'] = ride['taxi_id'] if ride['taxi_id'] != '' else None 

            ride['trip_start_timestamp'] = ride['trip_start_timestamp'] if ride['trip_start_timestamp'] != '' else None
            ride['trip_end_timestamp'] = ride['trip_end_timestamp'] if ride['trip_end_timestamp'] != '' else None
            
            ride['trip_seconds'] = int(ride['trip_seconds']) if ride['trip_seconds'] != '' and ride['trip_seconds'] != '0' else None
            ride['trip_miles'] = float(ride['trip_miles']) if ride['trip_miles'] != '' and ride['trip_miles'] != '0' else None

            ride['fare'] = float(ride['fare']) if ride['fare'] != '' and ride['fare'] != '0.00' else None
            ride['tips'] = float(ride['tips']) if ride['tips'] != '' and ride['tips'] != '0.00' else None
            ride['tolls'] = float(ride['tolls']) if ride['tolls'] != '' and ride['tolls'] != '0.00' else None
            ride['extras'] = float(ride['extras']) if ride['extras'] != '' and ride['extras'] != '0.00' else None

            ride['pickup_latitude'] = ride['pickup_latitude'] if ride['pickup_latitude'] != '' else None 
            ride['pickup_longitude'] = ride['pickup_longitude'] if ride['pickup_longitude'] != '' else None

            ride['dropoff_latitude'] = ride['dropoff_latitude'] if ride['dropoff_latitude'] != '' else None 
            ride['dropoff_longitude'] = ride['dropoff_longitude'] if ride['dropoff_longitude'] != '' else None

            if ride['pickup_latitude'] is None or ride['pickup_longitude'] is None:
                pickup_location = "null" 
            else:
                pickup_latitude = float(column_remapping['pickup_latitude'][ride['pickup_latitude']])
                pickup_longitude = float(column_remapping['pickup_longitude'][ride['pickup_longitude']])
                pickup_location = "ST_GeomFromText('POINT({} {})')".format(pickup_latitude, pickup_longitude)
           
            if ride['dropoff_latitude'] is None or ride['dropoff_longitude'] is None:
                dropoff_location = "null"
            else:
                dropoff_latitude = float(column_remapping['dropoff_latitude'][ride['dropoff_latitude']])
                dropoff_longitude = float(column_remapping['dropoff_longitude'][ride['dropoff_longitude']])
                dropoff_location = "ST_GeomFromText('POINT({} {})')".format(ride['dropoff_latitude'], ride['dropoff_longitude'])
           
            add_ride_query_with_points = add_ride_query.format(pickup_location, dropoff_location)
            
            to_insert = [
                ride['taxi_id'], 
                ride['trip_start_timestamp'],
                ride['trip_end_timestamp'],
                ride['trip_seconds'],
                ride['trip_miles'],
                ride['fare'],
                ride['tips'],
                ride['tolls'],
                ride['extras'],
                ride['payment_type'],
                ride['company']
            ]
            try:
                cursor.execute(
                    add_ride_query_with_points,
                    to_insert
                )
            except mysql.connector.Error as err:
                print(err)
        
        connection.commit()
cursor.close()
connection.close()