from databasedriver import DatabaseDriver
from queries import insert_business_table
from bizsearch import BizSearch


def main():
    b = BizSearch(term="pizza", location="33437", price="")
    db = DatabaseDriver()
    db.setup()

    all_inserts = [insert_business_table.format(*result) for result in b.get_results()]
    # for result in b.get_results():
    #     query = insert_business_table.format(*result)
    #     all_inserts.append(query)
    if all_inserts:
        bulk_insert = "BEGIN; \n" + '\n'.join(all_inserts) + "\nCOMMIT;"
        print(bulk_insert)
        db.execute_query(bulk_insert)

if __name__ == "__main__":
    main()
