import os
import sys
import mysql.connector
from mysql.connector import errorcode

from utils import make_connection, chunker


INDEXES = {
    'rides': ['miles', 'seconds', 'start_timestamp', 'end_timestamp', ('miles', 'seconds')],
    'payments': ['fare']
}

if __name__ == '__main__':
    drop_index = False 
    if len(sys.argv) > 1:
        if sys.argv[1] == "drop":
            drop_index = True

    connection = make_connection()
    cursor = connection.cursor()
    
    for table in INDEXES:
        print(f"Working on table {table}")
        for column in INDEXES[table]:
            if drop_index:
                print(f"\tDropping index on column {column}")       
                try:     
                    if isinstance(column, str):
                        cursor.execute(f'DROP INDEX `{column}` ON `{table}`')
                    else:
                        cursor.execute(f'DROP INDEX `{"_".join(column)}` ON `{table}`')                        
                except Exception as e:
                    print(e)
                    print("\tIndex does not exists")
            else:
                print(f"\tCreating index on column {column}")
                try:
                    if isinstance(column, str):
                        cursor.execute(f'ALTER TABLE `{table}` ADD INDEX `{column}` (`{column}`)')
                    else:
                        cursor.execute(f'ALTER TABLE `{table}` ADD INDEX `{"_".join(column)}` ({",".join(column)})')
                except Exception as e:
                    print(e)
                    print("\tIndex already exists")                    
