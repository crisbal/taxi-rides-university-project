import mysql.connector
from mysql.connector import errorcode

from utils import make_connection 

if __name__ == "__main__":
    connection = make_connection()
    ## create tables

    drop_table = "DROP TABLE IF EXISTS `{}`"

    TABLES = {}
    TABLES["rides"] = """
        CREATE TABLE `rides` (
            `id` int NOT NULL AUTO_INCREMENT,
            `taxi_id` int,
            `trip_start_timestamp` datetime,
            `trip_end_timestamp` datetime,      
            `trip_seconds` int,
            `trip_miles` decimal(10, 2),
            `fare` decimal(10, 2),
            `tips` decimal(10, 2),
            `tolls` decimal(10, 2),
            `extras` decimal(10, 2),
            `payment_type` text,
            `start_location` POINT,
            `end_location` POINT,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB
    """

    cursor = connection.cursor()

    for name, query in TABLES.items():
        try:
            print(f"Creating table {name}")
            cursor.execute(drop_table.format(name))
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(f"\t {err.msg}")
        else:
            print("\t OK")

    cursor.close()

    connection.close()

