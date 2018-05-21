import sys
import os
import json
from timeit import default_timer as timer
from statistics import mean, pstdev

import yaml
import mysql.connector
from mysql.connector import errorcode

from utils import make_connection, chunker

TRIES = 10

if __name__ == '__main__':
    indexed = "" 
    if len(sys.argv) > 1:
        if sys.argv[1] == "indexed":
            indexed = "-indexed"

    folder, _ = os.path.split(__file__)
    queries = yaml.load(open(folder + '/../queries.yml'))
    benchmarks = json.load(open(folder + '/../benchmarks.json'))
    
    connection = make_connection()
    cursor = connection.cursor()

    for query_id, query in enumerate(queries):
        if query.get('skip'):
            continue
        
        query_id = str(query_id)
        
        print(f"Benchmarking: {query['title']}")
        print(f"\t{query['mysql']}")

        benchmark = benchmarks.get(query_id, {})
        benchmark["title"] =  query['title']
        benchmark["mysql" + indexed] = {}

        times = []    
        for i in range(0, TRIES):
            start_time = timer()
            cursor.execute(query['mysql'])
            end_time = timer()
            times.append(end_time - start_time)
            for row in cursor:
                pass
        
        benchmark["mysql" + indexed]["times"] = times
        benchmarks[query_id] = benchmark
        print(f"First execution: {times[0]}")
        print(f"Second execution: {times[1]}")
        print(f"Average over {TRIES} execution: {mean(times)}")
        print(f"\tSTDEV: {pstdev(times)}")
        print()
        print("-" * 40)
        print()
    
    json.dump(benchmarks, open(folder + '/../benchmarks.json', 'w'))

