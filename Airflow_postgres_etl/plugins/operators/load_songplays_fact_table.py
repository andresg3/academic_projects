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

class LoadFactTable(BaseOperator):

    ui_color = '#ffcfdb'

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        self.file_path = file_path
        super().__init__(*args, **kwargs)



    def pg_conn(self):

        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh", schema="test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        return conn, cur


    def process_insert_songplays(self, cur, json_file):
    	# open log file
	    df = pd.read_json(json_file, lines=True)

	    # filter by NextSong action and convert ts column to datetime64[ms]
	    df = df[df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})

	    # insert songplay records
		# get songid and artistid from song and artist tables
	    for i, row in df.iterrows():
	        cur.execute(song_select, (row.song, row.artist, row.length))
	        results = cur.fetchone()

	        if results:
	            songid, artistid = results
	        else:
	            songid, artistid = None, None

	        # insert songplay record
	        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
	        cur.execute(songplay_table_insert, songplay_data)

	    print(f"Records inserted for file {json_file}")
	

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
            self.process_insert_songplays(cur, json_file)
            conn.commit()