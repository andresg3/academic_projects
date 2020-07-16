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
        # Categories example : 'categories': [{'alias': 'icecream', 'title': 'Ice Cream & Frozen Yogurt'}{...}]
        categories = []
        for cat in biz['categories']:
            categories.append(cat['title'])
        categories = ' '.join(categories)

        # Latitude and Longitude example: {'latitude': 26.528388, 'longitude': -80.1495119}
        latitude = biz['coordinates']['latitude']
        longitude = biz['coordinates']['longitude']

        # Location example: 'location': {'display_address': ['6643 W Boynton Beach Blvd', 'Boynton Beach, FL 33437']}
        location = ', '.join(biz['location']['display_address'])

        # Some business do not have key price  (KeyError: 'price')
        price = biz['price'] if "price" in biz else None

        row = [biz['id'], self._add_escape_character(biz['name']), biz['image_url'], biz['url'], biz['review_count'], categories, biz['rating'],
               latitude, longitude, price, location, biz['display_phone']]
        return row

    def _add_escape_character(self, data):
        # https://www.owasp.org/index.php/SQL_Injection
        return data.replace("'", "''")

    def get_results(self):
        results = []
        for biz in self._biz_list:
            results.append(self._parse_results(biz))
        return results
