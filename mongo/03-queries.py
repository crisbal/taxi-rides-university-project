import os
import json
from timeit import default_timer as timer
from statistics import mean, pstdev

import yaml
import demjson
from pymongo import MongoClient

TRIES = 10

if __name__ == '__main__':
    folder, _ = os.path.split(__file__)
    queries = yaml.load(open(folder + '/../queries.yml'))
    benchmarks = json.load(open(folder + '/../benchmarks.json'))
    
    client = MongoClient('localhost', 27017, username='root', password='password')
    db = client["taxiRides"]
    rides_collection = db["rides"]

    for query_id, query in enumerate(queries):
        query_id = str(query_id)
        
        print(f"Benchmarking: {query['title']}")
        param = demjson.decode(query['mongo']['query'])
        print(f"\tExecuting {query['mongo']['type']}")
        print(f"\t{param}")

        benchmark = benchmarks.get(str(query_id), {})
        benchmark["title"] =  query['title']
        benchmark["mongo"] = {}

        times = []
        result = None
        for i in range(0, TRIES):
            start_time = timer()
            if query['mongo']['type'] == 'aggregate':
                result = list(rides_collection.aggregate(param))
            elif query['mongo']['type'] == 'count':
                result = rides_collection.find(param).count()
            end_time = timer()
            times.append(end_time - start_time)

        benchmark["mongo"]["times"] = times
        benchmarks[query_id] = benchmark
        print(benchmarks)
        print(f"Result: {result}")
        print(f"First execution: {times[0]}")
        print(f"Second execution: {times[1]}")
        print(f"Average over {TRIES} execution: {mean(times)}")
        print(f"\tSTDEV: {pstdev(times)}")
        print()
        print("-" * 40)
        print()

    json.dump(benchmarks, open(folder + '/../benchmarks.json', 'w'))
    

