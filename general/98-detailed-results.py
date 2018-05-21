import os
import json
from statistics import mean, pstdev

if __name__ == '__main__':
    folder, _ = os.path.split(__file__)
    benchmarks = json.load(open(folder + '/../benchmarks.json'))

    for i, benchmark in benchmarks.items():
        print(f"Query #{int(i)+1}: {benchmark['title']}")
        
        print('MySQL')
        mean_mysql_noindex = mean(benchmark["mysql"]["times"])
        print(f'\tMean execution time: {mean_mysql_noindex}')
        print(f'\tMax execution time: {max(benchmark["mysql"]["times"])}')
        print(f'\tMin execution time: {min(benchmark["mysql"]["times"])}')

        if benchmark.get('mysql-indexed'):
            print('MySQL (Indexed)')
            mean_mysql_index = mean(benchmark["mysql-indexed"]["times"])
            print(f'\tMean execution time: {mean_mysql_index}')
            print(f'\tMax execution time: {max(benchmark["mysql-indexed"]["times"])}')
            print(f'\tMin execution time: {min(benchmark["mysql-indexed"]["times"])}')

            print(f'\tSpeedup with indexes: {round(mean_mysql_noindex/mean_mysql_index, 2)}x')
        
        print('Mongo')
        mean_mongo_noindex = mean(benchmark["mongo"]["times"])
        print(f'\tMean execution time: {mean_mongo_noindex}')
        print(f'\tMax execution time: {max(benchmark["mongo"]["times"])}')
        print(f'\tMin execution time: {min(benchmark["mongo"]["times"])}')

        if benchmark.get('mongo-indexed'):
            print('Mongo (Indexed)')
            mean_mongo_index = mean(benchmark["mongo-indexed"]["times"])
            print(f'\tMean execution time: {mean_mongo_index}')
            print(f'\tMax execution time: {max(benchmark["mongo-indexed"]["times"])}')
            print(f'\tMin execution time: {min(benchmark["mongo-indexed"]["times"])}')

            print(f'\tSpeedup with indexes: {round(mean_mongo_noindex/mean_mongo_index, 2)}x')
            
        
        print()
