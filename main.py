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



    # The records are many. The function below break the records into small manageable chunks
    def chunk(self,input_list, chunk_size):
        return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

    def check_string_value(self,input_string):
        if input_string:  # This checks if the string is not empty or None
            return True
        else:
            return False

    def execute(self):
        try:
            print('Establish DB Connection')
            db_connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)

            gmaps = googlemaps.Client(key=self.google_api_key)


            if db_connection.is_connected():
                print('DB Connection Successful')
                print('Fetching Data Points')
                sql_select_query = "SELECT latitude,longitude FROM reports.et_farm_gps ORDER BY id"
                cursor = db_connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print(cursor.rowcount,'Data points Returned')

                chunk_size = 500
                chunk_count = cursor.rowcount//chunk_size
                counter = 0;
                # Split the records into smaller groups

                print('Splitting Records Into Small Chunks')
                chunks = self.chunk(records, chunk_size)
                print('Splitting Complete')

                # Iterate through each small chunk
                for chunk in chunks:
                    counter+=counter+1
                    print ('Processing Chunk')
                    elevation_results = gmaps.elevation(chunk)

                    # Define the SQL query to insert data into the database
                    insert_query = "INSERT INTO reports.et_elevation_data (elevation, latitude, longitude) VALUES (%s, %s, %s)"

                    # Extract values from the data and create a list of tuples
                    insert_data = [(elevation_result['elevation'], elevation_result['location']['lat'], elevation_result['location']['lng']) for elevation_result in
                                   elevation_results]

                    # Execute the insert query with the list of tuples
                    cursor.executemany(insert_query, insert_data)
                    db_connection.commit()


        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            print('Closing DB Connection')
            if db_connection.is_connected():
                cursor.close()
                db_connection.close()
                print('DB Connection Closed')


if __name__ == '__main__':
    Process().execute()
