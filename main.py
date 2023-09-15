import googlemaps
import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
import gzip
from datetime import datetime


class Process:

    def __init__(self):
        # Mysql Database Settings
        self.host = os.environ.get('DB_HOST')
        self.database = os.environ.get('DB_SCHEMA')
        self.user = os.environ.get('DB_USER')
        self.password = os.environ.get('DB_PASSWORD')
        #google api key
        # self.google_api_key = os.environ.get('GOOGLE_API_KEY')
        self.google_api_key = 'AIzaSyAs3DUBpPBh91sn5SodQova9vHfMgEeu58'





    # The records are many. The function below break the records into small manageable chunks
    def chunk(self,input_list, chunk_size):
        return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

    def check_string_value(self,input_string):
        if input_string:  # This checks if the string is not empty or None
            return True
        else:
            return False

    def gen_file(self,df, filename, compressed_filename):
        # generate the filename using the current date and time
        df.to_csv(filename, index=False)
        # compress the CSV file using gzip
        with open(filename, 'rb') as f_in:
            with gzip.open(compressed_filename, 'wb') as f_out:
                f_out.writelines(f_in)

        # remove the original CSV file
        os.remove(filename)
        return compressed_filename

    def execute(self):
        try:
            print('Establish DB Connection')
            db_connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)

            gmaps = googlemaps.Client(key=self.google_api_key)


            if db_connection.is_connected():
                print('DB Connection Successful')
                print('Fetching Data Points')
                sql_select_query = "SELECT latitude,longitude FROM reports.et_farm_gps where processed =0 AND (latitude != 0.00000 AND longitude != 0.00000) ORDER BY id"

                cursor = db_connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records

                print(cursor.rowcount,'Data points Returned')

                chunk_size = 500
                chunk_count = cursor.rowcount//chunk_size
                counter = 0
                # Split the records into smaller groups

                print('Splitting Records Into Small Chunks')
                chunks = self.chunk(records, chunk_size)
                print('Splitting Complete')

                # Iterate through each small chunk
                for chunk in chunks:
                    counter+=counter+1
                    print ('Processing Chunk')

                    # Convert the coordinates to floats
                    converted_chunk = [(float(coord[0]), float(coord[1])) for coord in chunk]
                    # print(converted_chunk)
                    elevation_results = gmaps.elevation(converted_chunk)

                    # Define the SQL query to insert data into the database
                    insert_query = "INSERT INTO reports.et_elevation_data (elevation, latitude, longitude) VALUES (%s, %s, %s)"

                    # Extract values from the data and create a list of tuples
                    insert_data = [(elevation_result['elevation'], "{:.5f}".format(elevation_result['location']['lat']), "{:.5f}".format(elevation_result['location']['lng'])) for elevation_result in
                                   elevation_results]
                    # Execute the insert query with the list of tuples
                    cursor.executemany(insert_query, insert_data)
                    db_connection.commit()

                    # Flag records as processed
                    update_processed = 'update reports.et_farm_gps a join reports.et_elevation_data b on a.longitude = b.longitude and a.latitude= b.latitude and a.processed =0 set a.processed = 1;'
                    cursor.execute(update_processed)
                    db_connection.commit()

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            print('Closing DB Connection')
            if db_connection.is_connected():
                cursor.close()
                db_connection.close()
                print('DB Connection Closed')

    def prep(self):
        try:
            print('Establish DB Connection')
            db_connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)
            cursor = db_connection.cursor()


            if db_connection.is_connected():
                print('DB Connection Successful')

                print('Truncate staging Tables')
                truncate_gps_mapping_table = "truncate table  reports.gps_mapping;"
                cursor.execute(truncate_gps_mapping_table)
                db_connection.commit()

                truncate_et_farm_gps = "truncate table reports.et_farm_gps;"
                cursor.execute(truncate_et_farm_gps)
                db_connection.commit()

                truncate_et_elevation_data = "truncate table reports.et_elevation_data;"
                cursor.execute(truncate_et_elevation_data)
                db_connection.commit()

                print('Stage GPS Data + Event ID ')
                insert_stage_evnt_data = ("insert into reports.gps_mapping(event_id,latitude,longitude) select id,TRUNCATE(latitude,5),"
                                          "TRUNCATE(longitude,5) from adgg_uat.core_animal_event where country_id= 12 and event_type = 2 "
                                          "and latitude is not null and longitude is not null;")
                cursor.execute(insert_stage_evnt_data)
                db_connection.commit()

                print('Get Unique GPS From Staged GPS Data and Store')
                insert_unique_gps = "insert into reports.et_farm_gps(latitude,longitude) select distinct latitude,longitude from reports.gps_mapping;"
                cursor.execute(insert_unique_gps)
                db_connection.commit()

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            print('Closing DB Connection')
            if db_connection.is_connected():
                db_connection.commit()
                db_connection.close()
                print('DB Connection Closed')
    def merge(self):
        try:
            print('Establish DB Connection')
            db_connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)


            if db_connection.is_connected():
                print('DB Connection Successful')

                print('Fetch Elevation Data')
                query_elevation = "SELECT longitude,latitude,elevation FROM reports.et_elevation_data"
                df_elevation = pd.read_sql(query_elevation, db_connection)

                print('Fetch Events Mapping Data')
                query_events_mapping = "SELECT event_id,longitude,latitude FROM reports.gps_mapping"
                df_events = pd.read_sql(query_events_mapping, db_connection)

                print('Merge Elevation Data With Events IDs')
                merged_data = df_elevation.merge(df_events, on=['longitude', 'latitude'], how='inner')
                df_events['elevation'] = merged_data['elevation']

                print("Load Test Day CSV file into a DataFrame")
                csv_file_path = '/home/kosgei/Desktop/testday_lactation_combined_output.csv'
                df_test_day = pd.read_csv(csv_file_path)

                print('Merge Elevation Data With Test Day Data Using Event ID')
                merged_test_day_data = pd.merge(df_test_day, df_events, on='event_id', how='left')
                df_test_day['elevation'] = merged_test_day_data['elevation']

                print('Generate CSV Output')
                now = datetime.now()
                valid_output_csv = f"/home/kosgei/Desktop/testday-{now.strftime('%Y-%m-%d')}.csv"
                valid_output_gz = f"{valid_output_csv}.gz"
                self.gen_file(df_test_day, valid_output_csv, valid_output_gz)


        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            print('Closing DB Connection')
            if db_connection.is_connected():
                db_connection.commit()
                db_connection.close()
                print('DB Connection Closed')


if __name__ == '__main__':
    # Process().prep()
    # Process().execute()
    Process().merge()


