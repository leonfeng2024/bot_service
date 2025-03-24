from pymongo import MongoClient

class UserMongoTools:
    def __init__(self):
        self.client = MongoClient(
            host="127.0.0.1",
            port=27017,
            username="root",
            password="P@ssw0rd"
        )
        self.db = self.client['user']
        self.collection = self.db['userinfo']

    def verification(self, username: str, password: str) -> dict:
        existing_record = self.collection.find_one(
            {"username": username, "password": password}
        )
        if existing_record:
            return {'user_id': existing_record["id"], 'role': existing_record["role"]}
        else:
            return {'user_id': 0, 'role': None} 