import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

api_key = config['KEYS']['API_KEY']
header = {'Authorization': 'Bearer %s' % api_key}  # used by api_request.py