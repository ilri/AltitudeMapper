import googlemaps
import os
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import datetime


class Process:

    def __init__(self):
        # Mysql Database Settings
        self.host = os.environ.get('DB_HOST')
        self.database = os.environ.get('DB_SCHEMA')
        self.user = os.environ.get('DB_USER')
        self.password = os.environ.get('DB_PASSWORD')
        #google api key
        self.google_api_key = os.environ.get('GOOGLE_API_KEY')





    def execute(self):


        try:
            print(self.host)
            connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)

            if connection.is_connected():
                sql_select_query = "select id,longitude,latitude from reports.et_farm_gps order by id"
                cursor = connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print("Total Number Of Data Points: ", cursor.rowcount)
                requests = []
                # for record in records:
                #     record_id = record[0]
                #     longitude = record[1]
                #     latitude = record[2]
                #
                #
                #     sql_update_query = "UPDATE data_points SET is_processed =1 WHERE trim(id)={id}".format(
                #         id=record_id)
                #
                #
                #     cursor.execute(sql_update_query)
                #     connection.commit()

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()


if __name__ == '__main__':
    Process().execute()
