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
            new_path_version
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
            new_path_version
        """

        from bson.json_util import dumps
        import json
        from pymongo import MongoClient

        client = self.connect_metadata(username, password, database)

        # Get database 
        db = client[database] 
        
        # Created or Switched to collection names: Metadata.config_table 
        collection = db[table] 


        # Read Metadata
        connection_mongo = "mongodb+srv://admin:admin@mongo-cluster.r5jfxdp.mongodb.net/metadata?retryWrites=true&w=majority&appName=mongo-cluster"

        # Connection to MongoDB  
        try: 
            mongo_uri = connection_mongo
            client = MongoClient(mongo_uri)
            print("Connected successfully!!!") 
        except:   
            print("Could not connect to MongoDB") 

        # Connect Database 
        db = client.metadata 
        
        # Connect Metadata.config_table 
        collection = db.config_table 

        # Query data with phase: CusDB -> Bronze
        cursor = collection.find({phase})

        # Convert to json_data
        json_data = dumps(cursor, indent = 2)

        metadata_action = json.loads(json_data)

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
            new_path_version
        """

        client = self.connect_metadata(username, password, database)

        # Get database 
        db = client[database] 
        
        # Created or Switched to collection names: Metadata.config_table 
        collection = db[table] 


        return collection.insert_many(data_to_insert)