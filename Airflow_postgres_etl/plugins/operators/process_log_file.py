from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

import logging
import os
import glob
import pandas as pd

import sys
sys.path.insert(0, '/usr/local/airflow/scripts') # using docker-airflow
from sql_queries import *

log = logging.getLogger(__name__)

class ProcessLogOperator(BaseOperator):

    ui_color = '#A6E6A6'

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        self.file_path = file_path
        super().__init__(*args, **kwargs)


    def process_log_file(self, cur, json_file):
    	# open log file
	    df = pd.read_json(json_file, lines=True)

	    # filter by NextSong action and convert ts column to datetime64[ms]
	    df = df[df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})

	    # create time series of ts column
	    t = df['ts']

	    # create time_data dataframe using series created above
	    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
	    time_data = []
	    for time in t:
	        time_data.append([time, time.hour, time.day, time.weekofyear, time.month, time.year, time.day_name()])

	    time_df = pd.DataFrame(data=time_data, columns=column_labels)

	    # insert time_data df to time table
	    for i, row in time_df.iterrows():
	        cur.execute(time_table_insert, row)

	    # create user_df and insert to user table
	    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

	    for index, row in user_df.iterrows():
	        cur.execute(user_table_insert, row)

	    # insert songplay records
	    # get songid and artistid from song and artist tables
	    # for i, row in df.iterrows():
	    #     cur.execute(song_select, (row.song, row.artist, row.length))
	    #     results = cur.fetchone()

	    #     if results:
	    #         songid, artistid = results
	    #     else:
	    #         songid, artistid = None, None

	    #     # insert songplay record
	    #     songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
	    #     cur.execute(songplay_table_insert, songplay_data)

	    # print(f"Records inserted for file {json_file}")

    def pg_conn(self):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh", schema="test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        return conn, cur


    def execute(self, context):
        
        # create connection to postgres: conn, cur
        conn, cur = self.pg_conn()

        # get all files matching extension from directory
        all_files = []
        for root, dirs, files in os.walk(self.file_path):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))

        # get total number of files found
        num_files = len(all_files)
        log.info("%s files found in %s", num_files, self.file_path)

        # iterate over files and process
        for i, json_file in enumerate(all_files, 1):
            self.process_log_file(cur, json_file)
            conn.commit()
