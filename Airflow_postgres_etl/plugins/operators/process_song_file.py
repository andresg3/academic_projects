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

class ProcessSongOperator(BaseOperator):

    ui_color = '#A6E6A6'

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        self.file_path = file_path
        super().__init__(*args, **kwargs)
    

    def process_song_file(self, cur, json_file):
        # load file into dataframe
        df = pd.DataFrame([pd.read_json(json_file, typ='series', convert_dates=False)])

        # load df values into separate vars
        for val in df.values:
            num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = val

        # insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)

        # print(f"Records inserted for file {json_file}")
        log.info("Processed file %s", json_file)


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
        # for i, json_file in enumerate(all_files, 1):
        for json_file in all_files:
            self.process_song_file(cur, json_file)
            conn.commit()