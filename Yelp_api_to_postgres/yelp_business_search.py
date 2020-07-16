from databasedriver import DatabaseDriver
from api_request import Request
import requests
from queries import insert_business_table
from bizsearch import BizSearch


def main():
    b = BizSearch(term="pizza", location="33437", price="")


    db = DatabaseDriver()
    db.setup()

    # queries = [insert_business_table.format(*to_string(result)) for result in b.get_results()]
    all_inserts = []
    for result in b.get_results():
        query = insert_business_table.format(*result)
        all_inserts.append(query)

    bulk_insert = "BEGIN; \n" + '\n'.join(all_inserts) + "\nCOMMIT;"
    print(bulk_insert)
    db.execute_query(bulk_insert)


    # all_rows = []
    # for biz in business_data['businesses']:
    #     categories = []
    #     for category in biz['categories']:
    #         categories.append(category['title'])
    #
    #     categories = ' '.join(categories)
    #     latitude = biz['coordinates']['latitude']
    #     longitude = biz['coordinates']['longitude']
    #     location = ','.join(biz['location']['display_address'])
    #     price = biz['price'] if "price" in biz else None
        # data_dict = {"id": biz['id'], "name": biz['name'], "image_url": biz['image_url'], "url": biz['url'],
        #         "review_count": biz['review_count'], "categories": categories, "rating": biz['rating'],
        #         "latitude": latitude, "longitude": longitude, "price": biz['price'], "location": location,
        #         "display_phone": biz['display_phone']
        #         }
        # print(data_dict)


        # row = [biz['id'], biz['name'].replace("'", "''"), biz['image_url'], biz['url'], str(biz['review_count']), categories,
        #              str(biz['rating']), latitude, longitude, price, location, biz['display_phone']]

    #     query = insert_business_table.format(*row)
    #     all_rows.append(query)
    #
    # # print("BEGIN; \n" + '\n'.join(all_rows) + "\nCOMMIT;")
    # query_to_execute = "BEGIN; \n" + '\n'.join(all_rows) + "\nCOMMIT;"
    #
    # db.execute_query(query_to_execute)




if __name__ == "__main__":
    main()
