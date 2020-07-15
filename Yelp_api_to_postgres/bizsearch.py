from api_request import Request


class BizSearch:
    def __init__(self, term, location, price=None):
        self._param = {'term': term, 'location': location}
        if price:
            self._param['price'] = price
        self._url = 'https://api.yelp.com/v3/businesses/search'
        self._biz_list = self._search_biz()

    def _search_biz(self):
        biz_search_request = Request.get_content(url=self._url, param=self._param)
        return biz_search_request['businesses'] if biz_search_request is not None else []

    def _parse_results(self, biz):
        print(biz)



    def _add_escape_char(self, data):
        print('Inside _parse_results method!!!')
        return None

    def get_results(self):
        # print(self._biz_list['businesses'])
        for biz in self._biz_list:
            self._parse_results(biz)
