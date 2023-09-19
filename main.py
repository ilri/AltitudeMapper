import googlemaps
import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
import gzip
from datetime import datetime


class AltGen:

    def __init__(self, host, username, password, database, google_api_key):
        # Mysql Database Settings
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.conn = None

        # google api key
        self.google_api_key = google_api_key

    def db_connect(self):
        try:
            # Create the database connection
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database
            )
            print('Create the database connection')
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def db_close(self):
        if self.conn:
            self.conn.close()
            print('Close the database connection')

    def create_transactional_tables(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()
                print('create Database Objects')
                create_table_alt_elevation_data = "CREATE TABLE IF NOT EXISTS alt_elevation_data(id int NOT NULL AUTO_INCREMENT,longitude varchar(10) DEFAULT NULL,latitude varchar(10) DEFAULT NULL,elevation float DEFAULT NULL,PRIMARY KEY (id))"
                cursor.execute(create_table_alt_elevation_data)

                create_table_alt_test_day_events = """
                CREATE TABLE IF NOT EXISTS alt_test_day_events(
                  id int NOT NULL AUTO_INCREMENT,
                  event_id int DEFAULT NULL,
                  longitude varchar(20) DEFAULT NULL,
                  latitude varchar(20) DEFAULT NULL,
                  PRIMARY KEY (id))
                """
                cursor.execute(create_table_alt_test_day_events)

                create_table_alt_gps_points = """
                CREATE TABLE IF NOT EXISTS alt_gps_points(
                  id int NOT NULL AUTO_INCREMENT,
                  longitude varchar(20) DEFAULT NULL,
                  latitude varchar(20) DEFAULT NULL,
                  processed tinyint DEFAULT '0',
                  PRIMARY KEY (id))
                """
                cursor.execute(create_table_alt_gps_points)

                self.conn.commit()
                cursor.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def clear_transactional_tables(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()
                print('Truncate Transactional Tables')
                truncate_alt_elevation_data = "Truncate table alt_elevation_data"
                cursor.execute(truncate_alt_elevation_data)

                truncate_alt_gps_points = "Truncate table alt_gps_points"
                cursor.execute(truncate_alt_gps_points)

                truncate_alt_test_day_events = "Truncate table alt_test_day_events"
                cursor.execute(truncate_alt_test_day_events)

                self.conn.commit()
                cursor.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def fetch_gps_from_test_day(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()
                print('Fetch GPS + Event ID From Test Day Records')
                insert_stage_evnt_data =  """    
                insert into alt_test_day_events(event_id,latitude,longitude)           
                select a.id,TRUNCATE(c.latitude,5),TRUNCATE(c.longitude,5) 
                from adgg_uat.core_animal_event a
                join adgg_uat.core_animal b on a.animal_id = b.id
                join adgg_uat.core_farm c on b.farm_id = c.id
                where c.country_id= 12 and a.event_type = 2 
                and c.latitude is not null and c.longitude is not null
                """

                cursor.execute(insert_stage_evnt_data)
                self.conn.commit()
                cursor.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def get_unique_gps(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()
                print('Get Unique GPS From Staged GPS Data')
                insert_unique_gps = "insert into alt_gps_points(latitude,longitude) select distinct latitude,longitude from alt_test_day_events;"
                cursor.execute(insert_unique_gps)
                self.conn.commit()
                cursor.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    # The records are many. The function below break the records into small manageable chunks
    def chunk(self, input_list, chunk_size):
        return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

    def check_string_value(self, input_string):
        if input_string:  # This checks if the string is not empty or None
            return True
        else:
            return False

    def gen_file(self, df, filename, compressed_filename):
        # generate the filename using the current date and time
        df.to_csv(filename, index=False)
        # compress the CSV file using gzip
        with open(filename, 'rb') as f_in:
            with gzip.open(compressed_filename, 'wb') as f_out:
                f_out.writelines(f_in)

        # remove the original CSV file
        os.remove(filename)
        return compressed_filename

    def get_elevation(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()
                gmaps = googlemaps.Client(key=self.google_api_key)
                sql_select_query = "SELECT latitude,longitude FROM alt_gps_points where processed =0 AND (latitude != 0.00000 AND longitude != 0.00000) ORDER BY id"

                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records

                print(cursor.rowcount, 'Data points Returned')

                chunk_size = 500
                # chunk_count = cursor.rowcount // chunk_size
                counter = 0
                # Split the records into smaller groups

                print('Splitting Records Into Small Chunks')
                chunks = self.chunk(records, chunk_size)
                print('Splitting Complete')

                # Iterate through each small chunk
                for chunk in chunks:
                    counter += counter + 1
                    print('Processing Chunk')

                    # Convert the coordinates to floats
                    converted_chunk = [(float(coord[0]), float(coord[1])) for coord in chunk]
                    # print(converted_chunk)
                    elevation_results = gmaps.elevation(converted_chunk)

                    # Define the SQL query to insert data into the database
                    insert_query = "INSERT INTO reports.alt_elevation_data (elevation, latitude, longitude) VALUES (%s, %s, %s)"

                    # Extract values from the data and create a list of tuples
                    insert_data = [(elevation_result['elevation'], "{:.5f}".format(elevation_result['location']['lat']),
                                    "{:.5f}".format(elevation_result['location']['lng'])) for elevation_result in
                                   elevation_results]
                    # Execute the insert query with the list of tuples
                    cursor.executemany(insert_query, insert_data)
                    self.conn.commit()

                    # Flag records as processed
                    update_processed = 'update alt_gps_points a join alt_elevation_data b on a.longitude = b.longitude and a.latitude= b.latitude and a.processed =0 set a.processed = 1;'
                    cursor.execute(update_processed)
                    self.conn.commit()

            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def merge(self):
        if self.conn:
            try:
                cursor = self.conn.cursor()

                print('Fetch Elevation Data')
                query_elevation = "SELECT longitude,latitude,elevation FROM reports.alt_elevation_data"
                df_elevation = pd.read_sql(query_elevation,  self.conn)

                print('Fetch Events Mapping Data')
                query_events_mapping = """
                    SELECT a.event_id,a.longitude,a.latitude,
                    b.birthdate dob,d.label breed, 
                    dam.id dam_id, dam.tag_id dam_tag_id, dam.birthdate dam_dob,f.label dam_breed ,
                    sire.id sire_id, sire.tag_id sire_tag_id, sire.birthdate sire_dob,e.label sire_breed 
                    FROM reports.alt_test_day_events a 
                    join adgg_uat.core_animal_event c on a.event_id = c.id
                    join adgg_uat.core_animal b ON c.animal_id= b.id 
                    left join adgg_uat.core_master_list d on d.value = b.main_breed and d.list_type_id = 8
                    left join adgg_uat.core_animal sire on sire.id = b.sire_id
                    left join adgg_uat.core_animal dam on dam.id = b.dam_id
                    left join adgg_uat.core_master_list e on e.value = sire.main_breed and e.list_type_id = 8
                    left join adgg_uat.core_master_list f on f.value = dam.main_breed and f.list_type_id = 8
                """
                df_events = pd.read_sql(query_events_mapping,  self.conn)

                print('Merge Elevation Data With Events IDs')
                merged_data = df_elevation.merge(df_events, on=['longitude', 'latitude'], how='inner')
                df_events['elevation'] = merged_data['elevation']

                print("Load Test Day CSV file into a DataFrame")
                csv_file_path = '/home/kosgei/Desktop/testday_lactation_combined_output.csv'
                df_test_day = pd.read_csv(csv_file_path)

                print('Merge Elevation Data With Test Day Data Using Event ID')
                merged_test_day_data = pd.merge(df_test_day, df_events[['dob', 'breed', 'sex','dam_id','elevation']], on='event_id', how='left')
                # df_test_day['elevation'] = merged_test_day_data['elevation']
                print('Generate CSV Output')
                now = datetime.now()
                valid_output_csv = f"/home/kosgei/Desktop/testday-{now.strftime('%Y-%m-%d')}.csv"
                valid_output_gz = f"{valid_output_csv}.gz"
                self.gen_file(df_test_day, valid_output_csv, valid_output_gz)

            except mysql.connector.Error as err:
                print(f"Error: {err}")


if __name__ == '__main__':
    # Create an instance of the AltGen class
    alt_gen = AltGen(host=os.environ.get('DB_HOST'), username=os.environ.get('DB_USER'),
                     password=os.environ.get('DB_PASSWORD'),
                     database=os.environ.get('DB_SCHEMA'), google_api_key=os.environ.get('DB_GOOGLE_API_KEY'))

    # Connect to the database
    alt_gen.db_connect()

    # Create Database Objects
    alt_gen.create_transactional_tables()

    # Clear Transactional Tables
    alt_gen.clear_transactional_tables()

    #fetch GPS From Test Day
    alt_gen.fetch_gps_from_test_day()

    #get unique GPS
    alt_gen.get_unique_gps()

    #get elevation
    alt_gen.get_elevation()

    #merge Data sets and Generate files
    alt_gen.merge()

    # Close the database connection
    alt_gen.db_close()




