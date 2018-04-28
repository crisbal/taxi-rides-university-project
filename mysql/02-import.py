import os
import csv
from itertools import islice

import mysql.connector
from mysql.connector import errorcode

from utils import make_connection

connection = make_connection()
cursor = connection.cursor()

add_ride_query = """
    INSERT INTO `rides`
    (`taxi_id`, `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, `trip_miles`, `fare`, `tips`, `tolls`, `extras`, `payment_type`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

folder, _ = os.path.split(__file__)

FILES = ['2016_01']

for FILE in FILES:
    print(f"Importing {FILE} Rides")
    with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
        rides = csv.reader(csvfile)

        header = next(rides) # skip header

        for ride in islice(rides, 10000):
            ride = dict(zip(header, ride))

            ride['taxi_id'] = ride['taxi_id'] if ride['taxi_id'] != '' else None 

            ride['trip_start_timestamp'] = ride['trip_start_timestamp'] if ride['trip_start_timestamp'] != '' else None
            ride['trip_end_timestamp'] = ride['trip_end_timestamp'] if ride['trip_end_timestamp'] != '' else None
            
            ride['trip_seconds'] = int(ride['trip_seconds']) if ride['trip_seconds'] != '' and ride['trip_seconds'] != '0' else None
            ride['trip_miles'] = float(ride['trip_miles']) if ride['trip_miles'] != '' and ride['trip_miles'] != '0' else None

            ride['fare'] = float(ride['fare']) if ride['fare'] != '' and ride['fare'] != '0.00' else None
            ride['tips'] = float(ride['tips']) if ride['tips'] != '' and ride['tips'] != '0.00' else None
            ride['tolls'] = float(ride['tolls']) if ride['tolls'] != '' and ride['tolls'] != '0.00' else None
            ride['extras'] = float(ride['extras']) if ride['extras'] != '' and ride['extras'] != '0.00' else None

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
                ride['payment_type']
            ]

            try:
                cursor.execute(
                    add_ride_query, 
                    to_insert
                )
            except mysql.connector.Error as err:
                print(err)
        
        connection.commit()
cursor.close()
connection.close()
