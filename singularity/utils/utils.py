import re
from address import islocation
import hashlib
from pymongo import MongoClient, errors
import pandas as pd
import bson


# client = MongoClient( "mongodb://localhost:27017", serverSelectionTimeoutMS=5000, retryWrites=True)
# database = client["singularity"]
# collection = database["company_de"] 

# def cid(company_name):
#     # Define a list of German legal forms to remove
#     german_legal_forms = ['gmbh','ggmbh','ag', 'mbh', 'gbg', 'eg', 'ug', 'kg', 'ohg', 'partg', 'se', 'ev','ek','haftungsbeschränkt','gbr','co','kgaa','bs','cokg','gmbhcokg','mbhco','g','gmbhco','gesellschaft mit beschränkter haftung']
#     # Create a regular expression pattern to match the legal forms
#     pattern = r'\b(?:' + '|'.join(german_legal_forms) + r')\b'


#     company1 = company_name.split() 
#     company2 = [word for word in company1 if not islocation(word)]
#     company3 = ' '.join(company2)
#     company4 = re.sub(r'[^a-zA-Z\s0-9äöüÄÖÜß]', '', company3)
#     company5 = ' '.join(company4.split())

#     # Use re.sub to replace the matched legal forms with an empty string
#     without_legal_forms = re.sub(pattern, '', company5.lower(), flags=re.IGNORECASE)

#     # Step 1: Normalize the company name (convert to lowercase and remove spaces)
#     normalized_name = without_legal_forms.lower().replace(" ", "")

#     # Step 2: Create a hash using SHA-256
#     hash_object = hashlib.sha256(normalized_name.encode())
#     hashed_name = hash_object.hexdigest()
#     return hashed_name

#     # # Print the normalized name and the hash
#     # print("Normalized Name:", normalized_name)
#     # print("SHA-256 Hash:", hashed_name)


# def insert_location(postcode=None, file_path=None, column_name=None, country=None, data_frame=None):
#     try:
#         client = MongoClient( "mongodb://localhost:27017", serverSelectionTimeoutMS=5000, retryWrites=True)
#         database = client["singularity"]
#         collection = database["location_de"]
#         if isinstance(data_frame, pd.DataFrame) and column_name:
#             for location in data_frame[column_name]:
#                 try:
#                     if location:
#                         collection.insert_one({'_id':locationid(location),'location': location})
#                 except errors.DuplicateKeyError:
#                     continue
#     finally:
#         client.close()



    
def read_bson(file_path: str) -> dict:
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
    