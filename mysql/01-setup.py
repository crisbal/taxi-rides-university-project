"""
    This script will create the tables in the database
"""

import mysql.connector
from mysql.connector import errorcode

from utils import make_connection 

if __name__ == "__main__":
    connection = make_connection()
    ## create tables

    drop_table = "DROP TABLE IF EXISTS `{}`"

    TABLES = ["payment_types", "payments", "companies", "taxi_services", "rides"]
    DDL = {}

    DDL["payment_types"] = """
        CREATE TABLE `payment_types` (
            `id` int NOT NULL AUTO_INCREMENT,
            `name` text,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB
    """

    DDL["payments"] = """
        CREATE TABLE `payments` (
            `id` int NOT NULL AUTO_INCREMENT,
            `ride_id` int,
            `fare` decimal(10, 2),
            `tips` decimal(10, 2),
            `tolls` decimal(10, 2),
            `extras` decimal(10, 2),
            `total` decimal(10, 2) AS (NULLIF((IFNULL(`fare`, 0) + IFNULL(`tips`, 0) + IFNULL(`tolls`, 0) + IFNULL(`extras`, 0)), 0)),
            `payment_type_id` int,
            
            PRIMARY KEY (`id`),
            FOREIGN KEY (ride_id)
                REFERENCES rides(id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (payment_type_id)
                REFERENCES payment_types(id)
                ON UPDATE CASCADE ON DELETE CASCADE
        ) ENGINE=InnoDB
    """

    DDL["companies"] = """
        CREATE TABLE `companies` (
            `id` int NOT NULL AUTO_INCREMENT,
            `name` text,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB
    """

    DDL["taxi_services"] = """
        CREATE TABLE `taxi_services` (
            `id` int NOT NULL AUTO_INCREMENT,
            `taxi_id` int,
            `company_id` int,
            PRIMARY KEY (`id`),
            FOREIGN KEY (company_id)
                REFERENCES companies(id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            UNIQUE KEY `taxi_company` (`taxi_id`,`company_id`)
        ) ENGINE=InnoDB
    """

    DDL["rides"] = """
        CREATE TABLE `rides` (
            `id` int NOT NULL AUTO_INCREMENT,
            `taxi_service_id` int,
            `start_timestamp` datetime,
            `end_timestamp` datetime,      
            `seconds` int,
            `miles` decimal(10, 2),
            `start_location` POINT,
            `end_location` POINT,

            PRIMARY KEY (`id`),
            FOREIGN KEY (taxi_service_id)
                REFERENCES taxi_services(id)
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

