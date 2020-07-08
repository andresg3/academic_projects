import string

from databasedriver import DatabaseDriver
import requests
from queries import insert_business_table

api_key = 'F4WFoTggn_MJdVZBl9VbHCW_3uNHBe8_ML-JDLm51dFVUoD84nlYUenk6QHbAKTJD53cn9K4AL27IQCziyIZyzfDFv6IEe1GkCqLfqEWFM5RmyK0qclRE8SQ9t_zXnYx'

# def to_string(data):
#     return [str(value) for value in data.values()]

def main():
    # b = BusinessSearch(term="nnn", location="xxxx", price="xxxx")
    db = DatabaseDriver()
    db.setup()

    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'
    param = {'term': 'hair', 'location': '33437'}

    response = requests.get(url, headers=headers, params=param)

    if response.json() is not None:
        business_data = response.json()
    else:
        business_data = []

    print(business_data)

    all_rows = []
    for biz in business_data['businesses']:
        categories = []
        for category in biz['categories']:
            categories.append(category['title'])

        categories = ' '.join(categories)
        latitude = biz['coordinates']['latitude']
        longitude = biz['coordinates']['longitude']
        location = ','.join(biz['location']['display_address'])
        price = biz['price'] if "price" in biz else None
        # data_dict = {"id": biz['id'], "name": biz['name'], "image_url": biz['image_url'], "url": biz['url'],
        #         "review_count": biz['review_count'], "categories": categories, "rating": biz['rating'],
        #         "latitude": latitude, "longitude": longitude, "price": biz['price'], "location": location,
        #         "display_phone": biz['display_phone']
        #         }
        # print(data_dict)


        row = [biz['id'], biz['name'].replace("'", "''"), biz['image_url'], biz['url'], str(biz['review_count']), categories,
                     str(biz['rating']), latitude, longitude, price, location, biz['display_phone']]

        query = insert_business_table.format(*row)
        all_rows.append(query)

    # print("BEGIN; \n" + '\n'.join(all_rows) + "\nCOMMIT;")
    query_to_execute = "BEGIN; \n" + '\n'.join(all_rows) + "\nCOMMIT;"

    db.execute_query(query_to_execute)



if __name__ == "__main__":
    main()
