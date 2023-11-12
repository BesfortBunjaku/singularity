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
from website import from_google, from_clearbit,split_website,black
import time
import re
import hashlib
from address import islocation
from pymongo import errors
import math
import numpy as np
from phone import parse_phone

# LOGGING_FILE_PATH = os.getenv('CH_XSENDA_SCHIMUN_MAIN_LOG_ROOT_DIR') 
# DATABASE_NAME = os.getenv('CH_XSENDA_URSINA_MONGO_TESTGEN_DB')

# logger.add(sys.stderr, level="ERROR", format="<red>{time:YYYY-MM-DD HH:mm:ss.SSS}</red> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
# logger.add(f"{LOGGING_FILE_PATH}mongo.log", level="ERROR")

#DATA_DIR = os.getenv('CH_XSENDA_SCHIMUN_DATA_BACKUP_DIR')
TEMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")


# BACKUP_INFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup_info.json")

def is_blank(
    s,
    evallist=[
        "",
        0,
        "0",
        0.0,
        "0.0",
        None,
        "None",
        False,
        "False",
        math.nan,
        np.nan,
        "nan",
        "NAN",
        "n/a",
        "N/A",
        [],
        "[]",
        {},
        "{}",
        (),
        "()",
        set(),
        "set()",
    ],
):
    """Check if string is blank."""
    if type(s) == str:
        s = s.strip()
    return s in evallist

class Mongo:

    def __init__(self) -> None:
        self.client = MongoClient( "mongodb://localhost:27017", serverSelectionTimeoutMS=5000, retryWrites=True)
        self.database = self.client["singularity"]
        self.lstloc = self.list_of_locations()
        self.german_legal_forms = ['gmbh','ggmbh','ag', 'mbh', 'gbg', 'eg', 'ug', 'kg', 'ohg', 'partg', 'se', 'ev','ek','haftungsbeschränkt','gbr','co','kgaa','bs','cokg','gmbhcokg','mbhco','g','gmbhco','gesellschaft mit beschränkter haftung']
        self.pattern = r'\b(?:' + '|'.join(self.german_legal_forms) + r')\b'

    def list_of_locations(self):
        collection = self.database["location_de"]
        return collection.distinct("location")

    
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


    def insert(self):
        df = pd.read_json(r"C:\Users\besff\Downloads\de_companies_ocdata.jsonl.bz2",lines=True,compression='bz2',chunksize=1000)
        for chunk in df:
            chunk.fillna("", inplace=True)
            self.insert_documents(collection_name="data", documents=chunk.to_dict(orient='records'))
 
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
        collection = self.database["company_de"]
        return collection.count_documents({"websites.domain": {"$exists": True}} )
    
    def find_company(self, name):
        collection = self.database["company_de"]
        _id = self.cid(name)
        return collection.find_one({"_id": _id})
    
    def cid(self, company_name):
        # Define a list of German legal forms to remove
     
        company1 = company_name.split() 
        company2 = [word for word in company1 if word not in self.lstloc]
        company3 = ' '.join(company2)
        company4 = re.sub(r'[^a-zA-Z\s0-9äöüÄÖÜß]', '', company3)
        company5 = ' '.join(company4.split())

        # Use re.sub to replace the matched legal forms with an empty string
        without_legal_forms = re.sub(self.pattern, '', company5.lower(), flags=re.IGNORECASE)

        # Step 1: Normalize the company name (convert to lowercase and remove spaces)
        normalized_name = without_legal_forms.lower().replace(" ", "")

        # Step 2: Create a hash using SHA-256
        hash_object = hashlib.sha256(normalized_name.encode())
        hashed_name = hash_object.hexdigest()
        return hashed_name

        # # Print the normalized name and the hash
        # print("Normalized Name:", normalized_name)
        # print("SHA-256 Hash:", hashed_name)

    def insert_website(self, website):
        if is_blank(website):
            return
        swebsite = split_website(website)
        blacked = black(value=website)
        if is_blank(blacked) or is_blank(swebsite['domain']):      
            return
        return swebsite
    
    def insert_phone(self, phone_number):
        if is_blank(phone_number):
            return
        return parse_phone(phone_number)
    
    def fullname_case(self, fullname):
        try:
            person = {"fullname":"","firstname":"","lastname":""}
            if is_blank(fullname):
                return person
            person["fullname"] = fullname.strip()
            sfullname = fullname.split()
            if len(sfullname) == 1:
                person["firstname"] = sfullname[0].strip()
            
            elif len(sfullname) == 2:
                person["firstname"] = sfullname[0].strip()
                person["lastname"] = sfullname[1].strip()
            
            elif len(sfullname) == 3:
                person["firstname"] = sfullname[0].strip()
                person["lastname"] = sfullname[2].strip()
                
            else:
                person["firstname"] = sfullname[0].strip()
                person["lastname"] = sfullname[-1].strip()
            return person
        except IndexError:
            return person
    
    def fist_last_name_case(self, firstname,lastname):
        try:
            person = {"fullname":"","firstname":"","lastname":""}
            if is_blank(firstname) and is_blank(lastname):
                return person
            person["firstname"] = firstname.strip()
            person["lastname"] = lastname.strip()
            person["fullname"] = firstname.stri() + " " + lastname.strip()
            return person
        except IndexError:
            return person
    
    def insert_contact(self, doc):
        if doc.get("fullname", None):
            return self.fullname_case(doc["fullname"])
        if doc.get("firstname", None) and doc.get("lastname", None):
            return self.fist_last_name_case(doc["firstname"], doc["lastname"])
        return {"fullname":"","firstname":"","lastname":""}
    
    def insert_company(self, collection, hid, doc):
        contact = self.insert_contact(doc)

        company = {
               
                "company_number": "",
                "current_status": "",
                "jurisdiction_code": "de",
                "name": "",
                "officers": [
                    {
                    "name": "",
                    "other_attributes": {
                        "gender": "",
                        "city": "",
                        "firstname": "",
                        "flag": "",
                        "lastname": ""
                    },
                    "position": "",
                    "start_date": "",
                    "type": "person",
                    "email": "",
                    "linkedin": "",
                    "xing": "",
                    }
                ],
                "retrieved_at": {
                    "$date": ""
                },
                "previous_names": "",
                "subsequent_registrations": "",
                "alternate_registrations": "",
                "addresses": [
                    {
                    "street": "",
                    "house_number": "",
                    "postcode": "",
                    "city": ""
                    }
                ],
                "websites": [
                    {
                    "protocol": "https",
                    "subdomain": "",
                    "domain": "",
                    "tld": ""
                    }
                ],
                "email": "",
                "phones": [],
                "country": "",
                "founded": "",
                "industry": "",
                "linkedin": "",
                "xing": "",
                "locality": "",
                "region": "",
                "size": ""
                }
      
        company["name"] = doc["company_name"]
        company["retrieved_at"]["$date"] = ""
        company["country"] = "de"
        company["founded"] = doc.get("founded", "")
        company["industry"] = doc.get("industry", "")
        company["linkedin"] = doc.get("company_linkedin", "")
        company["xing"] = doc.get("company_xing", "")
        company["size"] = doc.get("size", "")
        company["email"] = doc.get("company_email", "")
        company["company_number"] = doc.get("company_number", "")
        company["current_status"] = doc.get("current_status", "")
        company["jurisdiction_code"] = doc.get("jurisdiction_code", "")
        company["previous_names"] = doc.get("previous_names", "")
        company["subsequent_registrations"] = doc.get("subsequent_registrations", "")
        company["alternate_registrations"] = doc.get("alternate_registrations", "")

        company["addresses"][0]["street"] = doc.get("street", "")
        company["addresses"][0]["house_number"] = doc.get("house_number", "")
        company["addresses"][0]["postcode"] = doc.get("postcode", "")
        company["addresses"][0]["city"] = doc.get("city", "")

        company["websites"] = [self.insert_website(doc.get("company_website", ""))]
        company["phones"] = [self.insert_phone(doc.get("company_phone", ""))]

        company["officers"][0]["name"] =  contact["fullname"]
        company["officers"][0]["other_attributes"]["gender"] = doc.get("gender", "")
        company["officers"][0]["other_attributes"]["firstname"] =  contact["firstname"]
        company["officers"][0]["other_attributes"]["flag"] = doc.get("flag", "")
        company["officers"][0]["other_attributes"]["lastname"] =  contact["lastname"]
        company["officers"][0]["position"] = doc.get("position", "")
        company["officers"][0]["start_date"] = doc.get("start_date", "")
        company["officers"][0]["type"] = doc.get("type", "person")
        company["officers"][0]["email"] = doc.get("person_email", "")
        company["officers"][0]["linkedin"] = doc.get("person_linkedin", "")
        company["officers"][0]["xing"] = doc.get("person_xing", "")

        collection.insert_one({'_id':hid, **company})            
 

    def update_website(self, collection, id, doc):
        if is_blank(doc.get("website", None)):
            return
        swebsite = split_website(doc["website"])
        blacked = black(value=doc['website'])
        if is_blank(blacked) or is_blank(swebsite['domain']):      
            return
        websites_exists = collection.find({"websites": {"$exists": True}})
        if websites_exists:
            collection.update_one({"_id": id}, {"$addToSet": {"websites": swebsite}})
        else:
            collection.update_one({"_id": id}, {"$set": {"websites": [swebsite]}})

    def update_phone(self,phone_number): #  collection, id, doc
        print(parse_phone(phone_number))

    def process_company(self, df=None, rename_columns=None):
        collection = self.database["company_de"]
        if rename_columns:
            df.rename(columns=rename_columns, inplace=True)
        df.fillna("", inplace=True)
        listofdicts = df.to_dict(orient='records')
        for doc in listofdicts:
            if is_blank(doc['company_name']):
                continue
            hid = self.cid(doc['company_name'])
            if collection.find_one({"_id": hid}):
                continue
                # self.update_website(collection, hid, doc)
            else:
                self.insert_company(collection, hid, doc)

 
    
if __name__ == "__main__":
    # 227905
    df = pd.read_csv(r"C:\Users\besff\OneDrive\Documents\GitHub\singularity\d1.csv")
   
    mongo = Mongo()
    # mongo.update_phone("0049 30 555 78 78 0")
    # ['name', 'website', 'phone', 'gender', 'contact', 'location']
    rc = {
    "name":"company_name", # Company name
    "":"founded", # When the company was founded type: date, format: YYYY-MM-DD, 
    "":"industry", # Industry of the company
    "":"company_linkedin", # Company LinkedIn URL
    "":"company_xing", # Company Xing URL
    "":"size", # Company size, how many employees
    "":"company_email", # Company email
    "":"company_number", # Company number (Handelsregisternummer)
    "":"current_status", # Current status of the company (e.g. active, inactive, dissolved, etc.)
    "":"jurisdiction_code", # Jurisdiction code (e.g. de, us, etc.)
    "":"previous_names", # Previous names of the company
    "":"subsequent_registrations", # Subsequent registrations of the company (e.g. if the company was registered in another country)
    "":"alternate_registrations", # Alternate registrations of the company (e.g. if the company was registered in another country)
    "":"street",  # Street name of the company
    "":"house_number", # House number of the company
    "":"postcode", # Postcode of the company
    "location":"city", # City of the company
    "website":"company_website", # Company website
    "phone":"company_phone", # Company phone number
    "contact":"fullname", # Full name of the person (e.g. John Doe)
    "gender":"gender", # gender of the peson
    "":"firstname", # First name of the person
    "":"flag", # Flag of the person (e.g. Dr., Prof., etc.) 
    "":"lastname", # Last name of the person
    "":"position", # Position of the person in the company
    "":"person_email", # Person email, personal email of the person (e.g. john.doe@gmail)
    "":"person_linkedin", # Person LinkedIn URL 
    "":"person_xing" # Person Xing URL 
    }
    # mongo.process_company(df=df, rename_columns=rc)
    
    # print(df.head())
    # l = mongo.insert(df=df)
    # print(mongo.count_websites())
    print(mongo.find_company("Comarch AG"))