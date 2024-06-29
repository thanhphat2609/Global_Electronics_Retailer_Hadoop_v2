class Metadata:
    def __init__(self) -> None:
        pass


    def connect_metadata(self, username, password, database):
        """
        Function connection to MongoDB metadata

        - Args:
            username: Username of MongoDB.
            password: Password for Username.
            database: Database in MongoDB.
            table: Table store metadata_action

        - Return
            client connection to MongoDB
        """


        connection = f"mongodb+srv://{username}:{password}@mongo-cluster.r5jfxdp.mongodb.net/{database}?retryWrites=true&w=majority&appName=mongo-cluster"


        from pymongo import MongoClient 
  
        try: 
            mongo_uri = connection
            client = MongoClient(mongo_uri)
            print("Connected successfully!!!") 
        except:   
            print("Could not connect to MongoDB") 
        
        return client

    def read_metadata_action(self, username, password, database, table, phase):
        """
        Function Read data from MongoDB metadata

        - Args:
            username: Username of MongoDB.
            password: Password for Username.
            database: Database in MongoDB.
            table: Table store metadata_action
            phase: What phase for get exact job

        - Return
            metadata_action
        """

        from bson.json_util import dumps
        import json
        from pymongo import MongoClient
        from airflow.models import Variable

        client = self.connect_metadata(username, password, database)

        # Get database 
        db = client[database] 
        
        # Created or Switched to collection names: Metadata.config_table 
        collection = db[table] 

        # Query data with phase: CusDB -> Bronze
        cursor = collection.find({"phase": phase})

        # Convert to json_data
        json_data = dumps(cursor, indent = 2)

        metadata_action = json.loads(json_data)

        Variable.set(key = "metadata_action", value = metadata_action, serialize_json = True)

        return metadata_action



    def insert_metadata_schema(self, username, password, database, table, data_to_insert):
        """
        Function Insert to MongoDB metadata

        - Args:
            username: Username of MongoDB.
            password: Password for Username.
            database: Database in MongoDB.
            table: Table store metadata_action
            data_to_insert: Insert data

        - Return
            insert new value
        """

        client = self.connect_metadata(username, password, database)

        # Get database 
        db = client[database] 
        
        # Created or Switched to collection names: Metadata.config_table 
        collection = db[table] 


        return collection.insert_many(data_to_insert)