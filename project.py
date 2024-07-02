import os
import pandas as pd
from datetime import datetime


folder_path = 'H:\Me\Data_Analysis\Data Cleaning\Cyclistic\dataset'
all_files = os.listdir(folder_path)

##############################################################################

# Ensure the folder exists, if not, create it
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Define the file name
log_file = 'log.txt'

# Construct the full file path
log_file_path = os.path.join(folder_path, log_file)

# Write the string to the text file
with open(log_file_path, 'w') as f:
    f.write( f' ##############################################################################\n[{datetime.now()}] : Starting ...\n\n')

##############################################################################

print("Start")

for file in all_files:

    df = pd.read_csv(f'H:\Me\Data_Analysis\Data Cleaning\Cyclistic\dataset\{file}')

    print(f"Reading file: {file}")

    rows_b = df.shape[0]

    with open(log_file_path, 'w') as f:
        f.write(f'Reading the file {file}\n\n**[Before cleaning]**\n\nHead of DF:\n{df.head()}\n\nInfo about DF:\n {df.info()}\n\nNull Info \n{df.isnull().sum()}\n\n\n')


    main_col = ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'day_of_week',
        'start_station_name', 'start_station_id', 'end_station_name',
        'end_station_id', 'user_type']

    print("Checking Columns name ")
    cols = df.columns

    for col in cols:
        if col not in main_col:
            if col == 'start_time':
                df = df.rename({"start_time": "started_at"}, axis=1)
            if col == 'end_time':
                df = df.rename({"end_time": "ended_at"}, axis=1)
            if col == 'member_casual' :
                df = df.rename({"member_casual": "user_type"}, axis=1)
            if col == 'usertype' :
                df = df.rename({"usertype": "user_type"}, axis=1)
            
            print("Deleting addtional columns")

            if col == 'start_lat' :
                df = df.drop(columns='start_lat')
            if col == 'start_lng' :
                df = df.drop(columns='start_lng')
            if col == 'end_lat' :
                df = df.drop(columns='end_lat')
            if col == 'end_lng' :
                df = df.drop(columns='end_lng')
            if col == 'day_of_week_id' :
                df = df.drop(columns='day_of_week_id')     
            if col == 'ride_length' :
                df = df.drop(columns='ride_length')              
            
    print("Updating the date columns")

    df['started_at'] = pd.to_datetime(df['started_at'], errors = 'coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors = 'coerce')

    if 'day_of_week' not in cols:
        df['day_of_week'] = df['started_at'].dt.day_name()
        

    print("Checking station name")

    if 'start_station_name' not in cols:
        df['start_station_name'] = 'station_' + df['start_station_id'].astype(str)

    if 'end_station_name' not in cols:
        df['end_station_name'] = 'station_' + df['end_station_id'].astype(str)
        
    if 'rideable_type' not in cols:
        df['rideable_type'] = 'General_bike'
        



    print("Deleting the null stations")

    df = df.dropna(subset=['start_station_name', 'start_station_id', 'end_station_name', 'end_station_id'])

    rows_a = df.shape[0]

    with open(log_file_path, 'w') as f:
        f.write(f'**[After cleaning]**\n\nHead of DF:\n{df.head()}\n\nInfo about DF:\n {df.info()}\n\nNull Info {df.isnull().sum()}\n\n{((rows_b - rows_a) / rows_b) *100}')

    print("Writing the file")

    df.to_csv(f'H:\Me\Data_Analysis\Data Cleaning\Cyclistic\dataset\cleaning\cleaning_{file}')

    with open(log_file_path, 'w') as f:
        f.write(f'Writing Successfully\n\n _________________________________________________\n\n')
    
    print("Done")

print("The End")