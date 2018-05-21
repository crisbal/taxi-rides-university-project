import os
import json
from statistics import mean, pstdev

import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':
    folder, _ = os.path.split(__file__)
    benchmarks = json.load(open(folder + '/../benchmarks.json'))

    fig, ax = plt.subplots()

    ind = np.arange(len(benchmarks)) # the x locations for the groups
    width = 0.1
    
    list_of_times = [
        benchmark.get('mysql', {}).get('times') for _, benchmark in benchmarks.items()
    ]
    means = [mean(times) if times else 0 for times in list_of_times]
    stds = [pstdev(times) if times else 0 for times in list_of_times]
    p_mysql = ax.bar(ind-width, means, width, color='#f44141', bottom=0, yerr=stds)
    
    mysql_index = False
    if any([benchmark.get('mysql-indexed', None) != None for _, benchmark in benchmarks.items()]):
        mysql_index = True
        list_of_times = [
            benchmark.get('mysql-indexed', {}).get('times') for _, benchmark in benchmarks.items()
        ]
        means = [mean(times) if times else 0 for times in list_of_times]
        stds = [pstdev(times) if times else 0 for times in list_of_times]
        p_mysql_index = ax.bar(ind, means, width, color='#41f446', bottom=0, yerr=stds)
         
    list_of_times = [
        benchmark.get('mongo', {}).get('times') for _, benchmark in benchmarks.items()
    ]
    means = [mean(times) if times else 0 for times in list_of_times]
    stds = [pstdev(times) if times else 0 for times in list_of_times]
    p_mongo = ax.bar(ind+width, means, width, color='#4146f4', bottom=0, yerr=stds)

    mongo_index = False
    if any([benchmark.get('mongo-indexed', None) != None for _, benchmark in benchmarks.items()]):
        mongo_index = True
        list_of_times = [
            benchmark.get('mongo-indexed', {}).get('times') for _, benchmark in benchmarks.items()
        ]
        means = [mean(times) if times else 0 for times in list_of_times]
        stds = [pstdev(times) if times else 0 for times in list_of_times]
        p_mongo_index = ax.bar(ind+width*2, means, width, color='#41f4eb', bottom=0, yerr=stds)

    
    ax.set_title('Queries Performance')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels([f"Query #{int(i)+1}" for i in benchmarks])
    ax.set_ylabel("Seconds")
    
    colors = [p_mysql[0], p_mongo[0]]
    labels = ['MySQL', 'Mongo']
    if mysql_index:
        colors.insert(1, p_mysql_index[0])
        labels.insert(1, 'MySQL Indexed')
    if mongo_index:
        colors.append(p_mongo_index[0])
        labels.append('Mongo Indexed')

    ax.legend(colors, labels)
    ax.autoscale_view()

    plt.show()
    fig.savefig(folder + '/../.images/benchmarks.png', dpi=fig.dpi*2)
