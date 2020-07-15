import requests
from auth import header

class Request:
    def __init__(self):
        self._header = header

    @staticmethod
    def get_content(url, param):
        response = requests.get(url, headers=header, params=param)
        biz_data = response.json()
        return biz_data

# def main():
#
#     business_search_request = Request.get_content(url_, param_)
#     print(business_search_request)
#     print(type(business_search_request))
#
#
# if __name__ == "__main__":
#     main()
