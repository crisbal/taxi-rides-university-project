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
    

    cursor.execute("SET AUTOCOMMIT=1")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    cursor.execute("SET UNIQUE_CHECKS=1")
    cursor.execute("SET SQL_MODE=''")
    cursor.close()
    connection.close()
