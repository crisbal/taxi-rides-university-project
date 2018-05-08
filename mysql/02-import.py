"""
    This script will do the importing of the actual data, companies and rides
"""

import os
import csv
import json
from itertools import islice

import mysql.connector
from mysql.connector import errorcode

from utils import make_connection, chunker

def clean_company(company):
    company = dict(zip(header, company))
    return [company['id'], company['name']]

def clean_ride(ride):
    ride = dict(zip(header, ride))

    ride['company'] = int(ride['company']) if ride['company'] != '' else None

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
        ride['pickup_location'] = None
    else:
        pickup_latitude = float(column_remapping['pickup_latitude'][ride['pickup_latitude']])
        pickup_longitude = float(column_remapping['pickup_longitude'][ride['pickup_longitude']])
        ride["pickup_location"] = "POINT({} {})".format(ride['pickup_latitude'], ride['pickup_longitude'])
   
    if ride['dropoff_latitude'] is None or ride['dropoff_longitude'] is None:
        ride['dropoff_location'] = None
    else:
        dropoff_latitude = float(column_remapping['dropoff_latitude'][ride['dropoff_latitude']])
        dropoff_longitude = float(column_remapping['dropoff_longitude'][ride['dropoff_longitude']])
        ride["dropoff_location"] = "POINT({} {})".format(ride['dropoff_latitude'], ride['dropoff_longitude'])

    return [
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
        ride['pickup_location'],
        ride['dropoff_location'],
        ride['company']
    ]


if __name__ == "__main__":
    connection = make_connection()
    cursor = connection.cursor()

    add_ride_query = """
        INSERT INTO `rides`
        (`taxi_id`, `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, `trip_miles`, `fare`, `tips`, `tolls`, `extras`, `payment_type`, `start_location`, `end_location`, `company_id`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s), ST_GeomFromText(%s), %s)
    """

    add_company_query = """
        INSERT INTO `companies`
        (`id`, `name`)
        VALUES (%s, %s)
    """

    folder, _ = os.path.split(__file__)

    column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))


    print("Importing companies.csv")
    with open(folder + '/../dataset/companies.csv') as csvfile:
        companies = csv.reader(csvfile)
        header = next(companies)  # skip header
        for companies_chunk in chunker(companies, 3):
            companies_chunk = [clean_company(company) for company in companies_chunk]
            cursor.executemany(
                add_company_query,
                companies_chunk
            )
        connection.commit()

    cursor.execute("SET AUTOCOMMIT=0")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    cursor.execute("SET UNIQUE_CHECKS=0")

    FILES = ['2016_01']
    for FILE in FILES:
        print(f"Importing {FILE} Rides")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.reader(csvfile)

            header = next(rides) # skip header

            i = 0
            CHUNKER_SIZE = 1000
            for rides_chunk in chunker(rides, CHUNKER_SIZE):
                rides_chunk = [clean_ride(ride) for ride in rides_chunk]
                
                i += CHUNKER_SIZE
                if (i % 10000 == 0):
                    print(f"Progress: {i}")

                try:
                    cursor.executemany(
                        add_ride_query,
                        rides_chunk
                    )
                except mysql.connector.Error as err:
                    print(err)

            connection.commit()

    cursor.execute("SET AUTOCOMMIT=1")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    cursor.execute("SET UNIQUE_CHECKS=1")
    cursor.close()
    connection.close()
