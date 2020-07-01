from databasedriver import DatabaseDriver
import requests
import pprint

api_key = 'F4WFoTggn_MJdVZBl9VbHCW_3uNHBe8_ML-JDLm51dFVUoD84nlYUenk6QHbAKTJD53cn9K4AL27IQCziyIZyzfDFv6IEe1GkCqLfqEWFM5RmyK0qclRE8SQ9t_zXnYx'


def main():
    # b = BusinessSearch(term="nnn", location="xxxx", price="xxxx")
    db = DatabaseDriver()
    db.setup()

    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'
    param = {'term': 'working cow', 'location': '33437'}

    response = requests.get(url, headers=headers, params=param)

    if response.json() is not None:
        business_data = response.json()
    else:
        business_data = []

    # print(business_data.keys())
    # pprint.pprint(business_data['businesses'][1])

    for biz in business_data['businesses']:
        categories = []
        for category in biz['categories']:
            categories.append(category['title'])
        categories = ' '.join(categories)
        latitude = biz['coordinates']['latitude']
        longitude = biz['coordinates']['longitude']
        location = ','.join(biz['location']['display_address'])

        data_dict = {"id": biz['id'], "name": biz['name'], "image_url": biz['image_url'], "url": biz['url'],
                "review_count": biz['review_count'], "categories": categories, "rating": biz['rating'],
                "latitude": latitude, "longitude": longitude, "price": biz['price'], "location": location,
                "display_phone": biz['display_phone']
                }
        print(data_dict)
        # TRY USING LIST INSTEAD OF DIC !!!!

        data_list = [biz['id'], biz['name'], biz['image_url'], biz['url'], biz['review_count'], categories,
                     biz['rating'], latitude, longitude, biz['price'], location, biz['display_phone']]
        print(data_list)


    # print("=========================================")
    # for biz in business_data['businesses']:
    #     categories = ' '.join([category['title'] for category in biz['categories']])
    #     print(categories)
    # print("=========================================")


if __name__ == "__main__":
    main()
