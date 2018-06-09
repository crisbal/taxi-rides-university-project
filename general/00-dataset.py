"""
    This script will generate the files necessary for easier importing of the 
    data
"""

import os
import csv
import json
from itertools import islice

if __name__ == "__main__":
    folder, _ = os.path.split(__file__)
    CONFIG = json.load(open(folder + '/../config.json'))

    column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))

    FILES = CONFIG['FILES']
    companies = {}
    taxi_services = {}
    payment_types = {}

    for FILE in FILES:
        print(f"Finding data in {FILE}")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.DictReader(csvfile)
            
            if CONFIG['IMPORT_LIMIT'] > 0:
                rides = islice(rides, CONFIG['IMPORT_LIMIT'])

            for ride in rides:
                taxi_id = None
                if ride['taxi_id'] != '': 
                    taxi_id = int(ride["taxi_id"])

                company_id = None
                if ride['company'] != '':
                    company_id = ride['company']
                    company_name = column_remapping['company'][company_id]
                    company_id = int(company_id)

                    companies[company_id] = company_name
                
                if (taxi_id, company_id) not in taxi_services:
                    taxi_services[(taxi_id, company_id)] = len(taxi_services) + 1

                if ride['payment_type'] != '' and ride['payment_type'] not in payment_types:
                    payment_types[ride['payment_type']] = len(payment_types) + 1
                
                if ride['payment_type'] != '' and ride['payment_type'] not in payment_types:
                    payment_types[ride['payment_type']] = len(payment_types) + 1


        print(f"Partially found {len(companies)} different companies")
        print(f"Partially found {len(payment_types)} different payment_types")

        
    with open(folder + '/../dataset/companies.csv', 'w') as csvfile:
        companies_writer = csv.writer(csvfile)
        companies_writer.writerow(['id', 'name'])

        for c_id, c_name in companies.items():
            companies_writer.writerow([c_id, c_name])

    with open(folder + '/../dataset/payment_types.csv', 'w') as csvfile:
        payment_types_writer = csv.writer(csvfile)
        payment_types_writer.writerow(['id', 'name'])

        for p_name, p_id in payment_types.items():
            payment_types_writer.writerow([p_id, p_name])

    with open(folder + '/../dataset/taxi_services.csv', 'w') as csvfile:
        taxis_writer = csv.writer(csvfile)
        taxis_writer.writerow(['id', 'taxi_id','company_id'])
        
        for service_tuple, i in taxi_services.items():
            tuple_list = list(service_tuple)
            tuple_list.insert(0, i)
            taxis_writer.writerow(tuple_list)
