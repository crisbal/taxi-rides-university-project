import mysql.connector
from mysql.connector import errorcode

from utils import make_connection 

if __name__ == "__main__":
    connection = make_connection()
    ## create tables

    drop_table = "DROP TABLE IF EXISTS `{}`"

    TABLES = ["companies", "rides"]
    DDL = {}

    DDL["companies"] = """
        CREATE TABLE `companies` (
            `id` int NOT NULL AUTO_INCREMENT,
            `name` text,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB
    """

    DDL["rides"] = """
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
            `company_id` int,

            PRIMARY KEY (`id`),            
            FOREIGN KEY (company_id)
                REFERENCES companies(id)
                ON UPDATE CASCADE ON DELETE CASCADE
        ) ENGINE=InnoDB
    """

    cursor = connection.cursor()

    cursor.execute("SET foreign_key_checks = 0")
    for table_name in TABLES:
        try:
            print(f"Creating table {table_name}")
            cursor.execute(drop_table.format(table_name))
            cursor.execute(DDL[table_name])
        except mysql.connector.Error as err:
            print(f"\t {err.msg}")
        else:
            print("\t OK")
    cursor.execute("SET foreign_key_checks = 1")
    cursor.close()

    connection.close()

