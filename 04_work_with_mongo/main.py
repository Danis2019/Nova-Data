from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime, timedelta, date
import json


class DbMongoClient:

    """
    Client to working with MongoDB.

    Methods
    -------
    __get_actual_users_ids(user_collection)
        Find and return ids of users which did event at last 14 days
    save_archived_users()
        archive not actual users

    """

    def __init__(self):

        HOST = "localhost"
        PORT = 27017
        self.client = MongoClient(f"mongodb://{HOST}:{PORT}/")
        self.client.admin.command('ping')


    @staticmethod
    def __get_actual_users_ids(user_collection: Collection) -> list:

        actual_clients_events = user_collection.find(
            {
                "event_time": {
                    "$gte": datetime.now() - timedelta(14)
                }
            }
        )
        ids = []
        for event in actual_clients_events:
            ids.append(event["user_id"])
        return ids


    def save_archived_users(self):
        db = self.client["my_database"]
        user_collection = db["user_events"]
        old_clients_events = user_collection.find(
            {
                "event_time": {
                    "$lt": datetime.now() - timedelta(14)
                },
                "user_info.registration_date": {
                    "$lt": datetime.now() - timedelta(30)
                }
            }
        )

        actual_users_ids = self.__get_actual_users_ids(user_collection)

        archive_collection = db["archived_users"]

        archived_user_ids = []

        for event in old_clients_events:
            if event["user_id"] not in actual_users_ids:
                
                if event["user_id"] in archived_user_ids:
                    print(f"user {event["user_id"]} already archived")
                else:
                    archive_collection.insert_one({
                        "user_id": event["user_id"],
                        "user_info": event["user_info"],
                    })
                    archived_user_ids.append(event["user_id"])

                user_collection.delete_one(event)


        report = {
            "date": date.today().strftime('%Y-%m-%d'),
            "archived_users_count": len(archived_user_ids),
            "archived_user_ids": archived_user_ids
        }

        with open(f"{report["date"]}.json", "w") as f:
            json.dump(report, f)
        

DbMongoClient().save_archived_users()