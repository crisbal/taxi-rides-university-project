from timeit import default_timer as timer
from statistics import mean, pstdev

import mysql.connector
from mysql.connector import errorcode

from utils import make_connection, chunker

TRIES = 10

queries = [
    """
        SELECT COUNT(*)
        FROM rides
        WHERE miles > 4
    """,
    """
        SELECT avg(fare) as avg_fare
        FROM payments
    """,
    """
        SELECT AVG(
            IFNULL(fare, 0) + 
            IFNULL(tips, 0) + 
            IFNULL(tolls, 0) + 
            IFNULL(extras, 0)
        ) as avg_total 
        FROM payments
        WHERE fare IS NOT NULL OR 
            tips IS NOT NULL OR
            tolls IS NOT NULL OR
            extras IS NOT NULL 
    """
]

if __name__ == '__main__':
    connection = make_connection()
    cursor = connection.cursor()

    for query in queries:
        print(f"Benchmarking {query}")
        times = []    
        for i in range(0, TRIES):
            start_time = timer()
            cursor.execute(query)
            end_time = timer()
            times.append(end_time - start_time)
            for row in cursor:
                pass
        print(f"First execution: {times[0]}")
        print(f"Second execution: {times[1]}")
        print(f"Average over {TRIES} execution: {mean(times)}")
        print(f"\tSTDEV: {pstdev(times)}")
        print()
        print("-" * 40)
        print()

