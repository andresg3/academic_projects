import requests
from auth import header

class Request:
    def __init__(self):
        self._header = header

    @staticmethod
    def get_content(url, param):
        response = requests.get(url, headers=header, params=param)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error on request. Response code: {response.status_code}")
            return None