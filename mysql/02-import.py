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

def make_company_row(company):
    return [company['id'], company['name']]

def clean_ride(ride, column_remapping):
    ride['fare'] = float(ride['fare']) if ride['fare'] != '' and ride['fare'] != '0.00' else None
    ride['tips'] = float(ride['tips']) if ride['tips'] != '' and ride['tips'] != '0.00' else None
    ride['tolls'] = float(ride['tolls']) if ride['tolls'] != '' and ride['tolls'] != '0.00' else None
    ride['extras'] = float(ride['extras']) if ride['extras'] != '' and ride['extras'] != '0.00' else None

    ride['trip_start_timestamp'] = ride['trip_start_timestamp'] if ride['trip_start_timestamp'] != '' else None
    ride['trip_end_timestamp'] = ride['trip_end_timestamp'] if ride['trip_end_timestamp'] != '' else None
    
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
        ride["pickup_location"] = "POINT({} {})".format(pickup_latitude, pickup_longitude)
   
    if ride['dropoff_latitude'] is None or ride['dropoff_longitude'] is None:
        ride['dropoff_location'] = None
    else:
        dropoff_latitude = float(column_remapping['dropoff_latitude'][ride['dropoff_latitude']])
        dropoff_longitude = float(column_remapping['dropoff_longitude'][ride['dropoff_longitude']])
        ride["dropoff_location"] = "POINT({} {})".format(dropoff_latitude, dropoff_longitude)
    
    ride['taxi_id'] = ride['taxi_id'] if ride['taxi_id'] != '' else None 

    return ride

def make_ride_row(ride):
    return [
        ride['taxi_id'], 
        ride['trip_start_timestamp'],
        ride['trip_end_timestamp'],
        ride['trip_seconds'],
        ride['trip_miles'],
        ride['pickup_location'],
        ride['dropoff_location']
    ]

def make_payment_row(ride, payments_remapping, ride_id):
    return [
        ride_id,
        ride['fare'],
        ride['tips'],
        ride['tolls'],
        ride['extras'],
        payments_remapping[ride['payment_type']]
    ]

if __name__ == "__main__":
    connection = make_connection()
    cursor = connection.cursor()
    cursor.execute("SET AUTOCOMMIT=0")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    cursor.execute("SET UNIQUE_CHECKS=0")
    cursor.execute("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'")

    add_payment_type_query = """
        INSERT INTO `payment_types`
        (`id`, `name`)
        VALUES (%s, %s)
    """

    add_payment_query = """
        INSERT INTO `payments`
        (`ride_id`, `fare`, `tips`, `tolls`, `extras`, `payment_type_id`)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    add_ride_query = """
        INSERT INTO `rides`
        (`taxi_id`, `start_timestamp`, `end_timestamp`, `seconds`, `miles`, `start_location`, `end_location`)
        VALUES (%s, %s, %s, %s, %s, ST_GEOMFROMTEXT(%s), ST_GEOMFROMTEXT(%s))
    """
    
    add_company_query = """
        INSERT INTO `companies`
        (`id`, `name`)
        VALUES (%s, %s)
    """

    add_taxi_query = """
        INSERT INTO `taxis`
        (`id`, `company_id`)
        VALUES (%s, %s)
    """


    folder, _ = os.path.split(__file__)
    column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))
    payments_remapping = {}

    print("Importing payment_types.csv")
    with open(folder + '/../dataset/payment_types.csv') as csvfile:
        payment_types = csv.DictReader(csvfile)
        for payment_type in payment_types:
            payments_remapping[payment_type['name']] = int(payment_type['id'])
            cursor.execute(
                add_payment_type_query,
                [ payment_type['id'], payment_type['name'] ]
            )
        connection.commit()
    
    print("Importing companies.csv")
    with open(folder + '/../dataset/companies.csv') as csvfile:
        companies = csv.DictReader(csvfile)
        for companies_chunk in chunker(companies, 3):
            companies_chunk = [make_company_row(company) for company in companies_chunk]
            cursor.executemany(
                add_company_query,
                companies_chunk
            )
        connection.commit()

    print("Importing taxis.csv")
    with open(folder + '/../dataset/taxis.csv') as csvfile:
        taxis = csv.DictReader(csvfile)
        for taxi in taxis:
            taxi['company_id'] = taxi['company_id'] if taxi['company_id'] != '' else None 
            cursor.execute(
                add_taxi_query,
                [ taxi['id'], taxi['company_id'] ]
            )
        connection.commit()


    FILES = ['2016_01']
    for FILE in FILES:
        print(f"Importing {FILE} Rides")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.DictReader(csvfile)

            i = 0 # for progress
            ride_id = 1
            CHUNKER_SIZE = 1000
            #rides = islice(rides, 100000)
            for rides_chunk in chunker(rides, CHUNKER_SIZE):
                i += CHUNKER_SIZE
                if (i % 10000 == 0):
                    print(f"Progress: {i}")

                rides_chunk = [clean_ride(ride, column_remapping) for ride in rides_chunk]
                payments_chunk = [make_payment_row(ride, payments_remapping, ride_id) for ride, ride_id in zip(rides_chunk, range(ride_id, ride_id + CHUNKER_SIZE + 1))]
                rides_chunk = [make_ride_row(ride) for ride in rides_chunk]
                ride_id += len(rides_chunk)
                
                # cursor.execute('set profiling = 1')
                try:
                    cursor.executemany(
                        add_ride_query,
                        rides_chunk
                    )
                    cursor.executemany(
                        add_payment_query,
                        payments_chunk
                    )
                    
                except mysql.connector.Error as err:
                    print(err)
                    # cursor.execute('show profiles')
                    # for row in cursor:
                    #    print(row)       

                

            connection.commit()

    cursor.execute("SET AUTOCOMMIT=1")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    cursor.execute("SET UNIQUE_CHECKS=1")
    cursor.execute("SET SQL_MODE=''")
    cursor.close()
    connection.close()
