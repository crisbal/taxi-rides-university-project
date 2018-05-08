"""
    This script will generate the files necessary for easier importing of the 
    data
"""

import os
import csv
import json

if __name__ == "__main__":
    folder, _ = os.path.split(__file__)

    column_remapping = json.load(open(folder + '/../dataset/column_remapping.json'))

    FILES = ['2016_01', '2016_02', '2016_03', 
            '2016_04', '2016_05', '2016_06', 
            '2016_07', '2016_08', '2016_09',
            '2016_10', '2016_11', '2016_12']
    FILES = ['2016_01', '2016_02', '2016_03']

    companies = {}
    taxis = {}
    payment_types = {}

    for FILE in FILES:
        print(f"Finding data in {FILE}")
        with open(folder + '/../dataset/chicago_taxi_trips_' + FILE + '.csv') as csvfile:
            rides = csv.DictReader(csvfile)
            for ride in rides:
                if ride['taxi_id'] != '': 
                    taxi_id = ride["taxi_id"] = int(ride["taxi_id"])
                    taxis[taxi_id] = { "id": taxi_id }

                if ride['company'] != '':
                    company_id = ride['company']
                    company_name = column_remapping['company'][company_id]
                    company_id = int(company_id)

                    companies[company_id] = company_name

                    taxis[taxi_id]["company_id"] = company_id
                

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

    with open(folder + '/../dataset/taxis.csv', 'w') as csvfile:
        taxis_writer = csv.DictWriter(csvfile, fieldnames=['id','company_id'])
        taxis_writer.writeheader()
        for i, taxi in taxis.items():
            taxis_writer.writerow(taxi)
