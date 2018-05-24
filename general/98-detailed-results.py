import os
import json
from statistics import mean, pstdev

if __name__ == '__main__':
    folder, _ = os.path.split(__file__)
    benchmarks = json.load(open(folder + '/../benchmarks.json'))

    for i, benchmark in benchmarks.items():
        print(f"Query #{int(i)+1}: {benchmark['title']}")
        
        print('* MySQL:')
        mean_mysql_noindex = mean(benchmark["mysql"]["times"])
        print(f'    * Mean execution time: {round(mean_mysql_noindex, 3)}')
        print(f'    * Max execution time: {round(max(benchmark["mysql"]["times"]), 3)}')
        print(f'    * Min execution time: {round(min(benchmark["mysql"]["times"]), 3)}')

        if benchmark.get('mysql-indexed'):
            print('    * MySQL (Indexed)')
            mean_mysql_index = mean(benchmark["mysql-indexed"]["times"])
            print(f'        * Mean execution time: {round(mean_mysql_index, 3)}')
            print(f'        * Max execution time: {round(max(benchmark["mysql-indexed"]["times"]), 3)}')
            print(f'        * Min execution time: {round(min(benchmark["mysql-indexed"]["times"]), 3)}')

            print(f'    * Speedup with indexes: {round(mean_mysql_noindex/mean_mysql_index, 2)}x')
        
        print('* Mongo')
        mean_mongo_noindex = mean(benchmark["mongo"]["times"])
        print(f'    * Mean execution time: {round(mean_mongo_noindex, 3)}')
        print(f'    * Max execution time: {round(max(benchmark["mongo"]["times"]), 3)}')
        print(f'    * Min execution time: {round(min(benchmark["mongo"]["times"]), 3)}')

        if benchmark.get('mongo-indexed'):
            print('    * Mongo (Indexed)')
            mean_mongo_index = mean(benchmark["mongo-indexed"]["times"])
            print(f'        * Mean execution time: {round(mean_mongo_index, 3)}')
            print(f'        * Max execution time: {round(max(benchmark["mongo-indexed"]["times"]), 3)}')
            print(f'        * Min execution time: {round(min(benchmark["mongo-indexed"]["times"]), 3)}')

            print(f'    * Speedup with indexes: {round(mean_mongo_noindex/mean_mongo_index, 2)}x')

        print('* MySQL vs Mongo:')
        print(f'    * MySQL is {round(mean_mongo_noindex/mean_mysql_noindex, 2)} times faster than MongoDB')    
        if benchmark.get('mysql-indexed') and benchmark.get('mongo-indexed'):
            mean_mysql_index = mean(benchmark["mysql-indexed"]["times"])
            mean_mongo_index = mean(benchmark["mongo-indexed"]["times"])
            print(f'    * MySQL (Indexed) is {round(mean_mongo_index/mean_mysql_index, 2)} times faster than MongoDB (Indexed)')

             
             
        print()
