"""
    This script will do the importing of the actual data, companies and rides
"""

import os
import csv
import json
from itertools import islice
from collections import defaultdict
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
    
    ride['taxi_id'] = int(ride['taxi_id']) if ride['taxi_id'] != '' else None 
    ride['company'] = int(ride['company']) if ride['company'] != '' else None 

    return ride

def make_ride_row(ride, taxi_services_dict):
    return [
        taxi_services_dict[(ride['taxi_id'], ride['company'])], 
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
    folder, _ = os.path.split(__file__)
    
    CONFIG = json.load(open(folder + '/../config.json'))

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
        (`taxi_service_id`, `start_timestamp`, `end_timestamp`, `seconds`, `miles`, `start_location`, `end_location`)
        VALUES (%s, %s, %s, %s, %s, ST_GEOMFROMTEXT(%s), ST_GEOMFROMTEXT(%s))
    """
    
    add_company_query = """
        INSERT INTO `companies`
        (`id`, `name`)
        VALUES (%s, %s)
    """

    add_taxi_service_query = """
        INSERT INTO `taxi_services`
        (`id`, `taxi_id`, `company_id`)
        VALUES (%s, %s, %s)
    """


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
    n_companies = 0
    with open(folder + '/../dataset/companies.csv') as csvfile:
        companies = csv.DictReader(csvfile)
        for companies_chunk in chunker(companies, 3):
            companies_chunk = [make_company_row(company) for company in companies_chunk]
            cursor.executemany(
                add_company_query,
                companies_chunk
            )
            n_companies += 3
        connection.commit()
    print(f"Imported {n_companies} companies")

    print("Importing taxi_services.csv")
    n_taxi_services = 0
    taxi_services_dict = {}
    with open(folder + '/../dataset/taxi_services.csv') as csvfile:
        taxi_services = csv.DictReader(csvfile)
        for taxi_service in taxi_services:
            taxi_service['taxi_id'] = int(taxi_service['taxi_id']) if taxi_service['taxi_id'] != '' else None
            taxi_service['company_id'] = int(taxi_service['company_id']) if taxi_service['company_id'] != '' else None
            taxi_service['id'] = int(taxi_service['id']) if taxi_service['id'] != '' else None
            cursor.execute(
                add_taxi_service_query,
                [ taxi_service['id'], taxi_service['taxi_id'], taxi_service['company_id'] ]
            )
            taxi_services_dict[(taxi_service['taxi_id'], taxi_service['company_id'])] = taxi_service['id']
            n_taxi_services += 1
        connection.commit()
    print(f"Imported {n_taxi_services} taxi_services")
    
    FILES = CONFIG['FILES']
    for FILE in FILES:
        print(f"Importing {FILE} Rides")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.DictReader(csvfile)

            i = 0 # for progress
            ride_id = 1
            CHUNKER_SIZE = 1000
            if CONFIG['IMPORT_LIMIT'] > 0:
                rides = islice(rides, CONFIG['IMPORT_LIMIT'])
            for rides_chunk in chunker(rides, CHUNKER_SIZE):
                rides_chunk = [clean_ride(ride, column_remapping) for ride in rides_chunk]
                payments_chunk = [make_payment_row(ride, payments_remapping, ride_id) for ride, ride_id in zip(rides_chunk, range(ride_id, ride_id + CHUNKER_SIZE + 1))]
                rides_chunk = [make_ride_row(ride, taxi_services_dict) for ride in rides_chunk]
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

                i += CHUNKER_SIZE
                if (i % 10000 == 0):
                    print(f"Progress: {i}")

            connection.commit()

    cursor.execute("SET AUTOCOMMIT=1")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    cursor.execute("SET UNIQUE_CHECKS=1")
    cursor.execute("SET SQL_MODE=''")
    cursor.close()
    connection.close()
