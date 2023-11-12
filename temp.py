from pymongo import MongoClient
# from .utils import latest_date, delta_days, add_days, connection_string
from datetime import datetime
import json
import os
import sys
# from loguru import logger
# from dotenv import load_dotenv
import shutil
import bson
import pandas as pd
# load_dotenv()
from website import from_google, from_clearbit 
import time

# LOGGING_FILE_PATH = os.getenv('CH_XSENDA_SCHIMUN_MAIN_LOG_ROOT_DIR') 
# DATABASE_NAME = os.getenv('CH_XSENDA_URSINA_MONGO_TESTGEN_DB')

# logger.add(sys.stderr, level="ERROR", format="<red>{time:YYYY-MM-DD HH:mm:ss.SSS}</red> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
# logger.add(f"{LOGGING_FILE_PATH}mongo.log", level="ERROR")

#DATA_DIR = os.getenv('CH_XSENDA_SCHIMUN_DATA_BACKUP_DIR')
TEMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")


# BACKUP_INFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup_info.json")
 
class Mongo:

    def __init__(self) -> None:
        self.client = MongoClient( "mongodb://localhost:27017", serverSelectionTimeoutMS=5000, retryWrites=True)
        self.database = self.client["singularity"]


    @property
    def is_connected(self):
        """ Property to check if the connection is established or not

        Returns:
            bool: True if the connection is established else False

        Examples:
            >>> mongo = Mongo()
            >>> mongo.is_connected
            True
        """
        return True if self.client else False
    
    
    def get_collections(self) -> list:
        """ Get the raw collections in the database, this method is private and should not be used directly

        Returns:
            list: List of raw collections in the database
        
        Examples:
            >>> mongo = Mongo()
            >>> mongo.get_raw_collections()
        """        
        return [collection for collection in self.database.list_collection_names() if not collection.startswith("system")]
    
 
    def rename_collections(self, collection_names: dict = None) -> None:
        """ Rename the collections in the database

        Args:
            collection_names (dict, optional): _description_. Defaults to None.

        Examples:
            >>> mongo = Mongo()
            >>> mongo.rename_collections(collection_names = {"cx_process_raw": "cx_process"})
        """
        try:
            for old_name, new_name in collection_names.items():
                self.database[old_name].rename(new_name)
        finally:
            # Close the connection
            self.client.close()

    def write_documents(self, document_list, file_path):
        """Write a list of documents to a bson file."""
        with open(file_path, 'wb+') as f:
            for doc in document_list:
                f.write(bson.BSON.encode(doc))

    def update_backup_info(self):
        """Update the last backup timestamp in the backup_info.json file."""
     

        with open(BACKUP_INFO_PATH, 'r') as f:
            backup_info = json.load(f)

        backup_info['last_backup'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        with open(BACKUP_INFO_PATH, 'w') as f:
            json.dump(backup_info, f, default=str)
    
    

    def backup(self) -> None:
        """ Save the collections in the data local dir as JSON files, 
        this method is private and should not be used directly

        Examples:
            >>> mongo = Mongo()
            >>> mongo.backup_database()
        """        

        try:
            print("Backing up database...")
            # Create a temporary directory if it doesn't exist
            if not os.path.exists(TEMP_DIR):
                os.makedirs(TEMP_DIR)

            for collection_name in self.get_collections():
                self.backup_collection(collection_name)

            self.move_backup_files()
            # self.update_backup_info()


        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            raise e
        finally:
            # Clean up temporary directory
            if os.path.exists(TEMP_DIR):
                shutil.rmtree(TEMP_DIR)

    def backup_collection(self, collection_name: str) -> None:
        collection = self.database[collection_name]
        documents = list(collection.find())

        # Define the temporary bson file path
        temp_file_path = os.path.join(TEMP_DIR, f'{collection_name}.bson')

        # Write the documents to the temporary bson file
        self.write_documents(documents, temp_file_path)
        


    def move_backup_files(self) -> None:
        for collection_name in self.get_collections():
            temp_file_path = os.path.join(TEMP_DIR, f'{collection_name}.bson')
            final_file_path = os.path.join(DATA_DIR, f'{collection_name}.bson')
            shutil.move(temp_file_path, final_file_path)
    
    def restore(self) -> None:
        """ Restore the collections from the data local dir

        Examples:
            >>> mongo = Mongo()
            >>> mongo.restore()
        """
        try:
            for collection_name in self.__get_raw_collections():
                # Define the input JSON file path
                input_file_path = DATA_DIR + f'\{collection_name}.bson'
                # Read the bson file
                bson_objects = self.read_bson(file_path=input_file_path)
                # Remove the documents from the collection
                self.remove_documents(collection_name=collection_name)
                # Insert the documents in the collection
                self.insert_documents(collection_name=collection_name, documents=bson_objects)
        finally:
            # Close the connection
            self.client.close()
 
    
    def collections_exists(self) -> bool:
        for collection_name in self.__get_raw_collections():
            if not os.path.exists(DATA_DIR + f'\{collection_name}.bson'):
               raise Exception(f"Collection {collection_name} does not exist")
       
    
    def read_bson(self, file_path: str) -> dict:
        """ Read the bson file

        Args:
            file_path (str): File path

        Returns:
            dict: Dictionary of the bson file
        
        Examples:
            >>> mongo = Mongo()
            >>> mongo.read_bson(file_path = DATA_DIR + f'\cx_process_raw.bson')
        """        
        with open(file_path, 'rb') as f:
            data = bson.decode_all(f.read())
        return data
    
 
    
    def generate_data(self, date_key: str = "ts_date_created") -> None:
        try:
            
            self.backup()
            # self.collections_exists()

            for collection_name in self.__get_raw_collections():
                self.process_collection(collection_name, date_key)

        except Exception as e:
            logger.error(e)
            raise e
        finally:
            self.client.close()

    def process_collection(self, collection_name: str, date_key: str):
        collection = self.database[collection_name]
        ld = latest_date(collection=collection, date_key=date_key).strftime("%Y-%m-%d")
        diff_days = delta_days(date_str2=ld)
        self.process_json(collection_name, date_key, diff_days)

    def process_json(self, collection_name, date_key: str, diff_days: int):
        bson_objects = self.read_bson(file_path=DATA_DIR + f'\{collection_name}.bson')
        for bson_object in bson_objects:
            changed_date = add_days(date_time=bson_object[date_key], days_to_add=diff_days)
            bson_object[date_key] = changed_date

        self.remove_documents(collection_name=collection_name)
        self.insert_documents(collection_name=collection_name, documents=bson_objects)


     
    def insert_documents(self, collection_name: str, documents: list) -> None:
        """ Insert the documents in the collection

        Args:
            collection_name (str): Collection name
            documents (list): List of documents

        Examples:
            >>> mongo = Mongo()
            >>> mongo.insert_documents(collection_name = "cx_process_raw", documents = [{"name": "test"}])
        """        
      
        collection = self.database[collection_name]
        collection.insert_many(documents)


    def insert(self):
        df = pd.read_json(r"C:\Users\besff\Downloads\de_companies_ocdata.jsonl.bz2",lines=True,compression='bz2',chunksize=1000)
        for chunk in df:
            chunk.fillna("", inplace=True)
            self.insert_documents(collection_name="data", documents=chunk.to_dict(orient='records'))
     

    def remove_documents(self,collection_name) -> None:
        collection = self.database[collection_name]
        collection.delete_many({})
 

    def info(self):
        """ Get the info of the database

        Returns:
            dict: Dictionary of the database info
        
        Examples:
            >>> mongo = Mongo()
            >>> mongo.info()
        """
        with open(BACKUP_INFO_PATH, "r") as f:
            backup_info = json.load(f)
        db_info = self.database.command("dbstats")
        db_info.update(backup_info)
        db_info["backup_dir"] =  DATA_DIR
        db_info["log_dir"] =  LOGGING_FILE_PATH 
        return db_info


    def insert_website(self,id, website):
        collection = self.database["data"]
        collection.update_one({"_id": id}, {"$set": {"website": website}})
 
    def process_website(self):
        collection = self.database["data"]
        for doc in collection.find({"website": {"$exists": False}}):
            try:
                website = from_google(doc["name"],pause=5,error=True)
                if website:
                    self.insert_website(doc["_id"], website) 
            except IndexError:
                print("IndexError", doc["name"])
                continue

    def process_from_clearbit(self):
        collection = self.database["data"]
        for doc in collection.find({"website": {"$exists": False}}):
            try:
                website = from_clearbit(doc["name"])
                if website:
                    self.insert_website(doc["_id"], website)
            except KeyError:
                continue
            except IndexError:
                continue
    
    def count_websites(self):
        collection = self.database["data"]
        return collection.count_documents({"website": {"$exists": True}} )
    
    def find_company(self, name):
        collection = self.database["data"]
        return collection.find_one({"name" : {"$regex" : name, "$options" : "i"}})

    def insert_company(self, company):
        collection = self.database["data"]
        collection.insert_one(company)

    def insert_dataframe(self):
        import pandas as pd
        collection = self.database["data"]
        df = pd.read_csv(r"C:\Users\besff\OneDrive\Desktop\DE Company\Free Germany Business List.csv.csv")
        df = df.drop(['id'], axis=1)
        df = df.fillna('')
        df = df.to_dict('records')
        collection.insert_many(df)
    
if __name__ == "__main__":
    from address import from_text
    # 2569
    # 432774
    # 437012
    # mongo = Mongo()
    # collections = mongo.backup()
    # print(collections)
    #mongo.insert_dataframe()
    #mongo.process_website()
    # count = mongo.count_websites()
    # print(count)