
# imports for connections
import os
from  astrapy import DataAPIClient, Database
from dotenv import load_dotenv

# imports for json insert
from astrapy import Database, Collection
from astrapy.constants import VectorMetric
from astrapy.info import CollectionVectorServiceOptions
import json
from typing import Callable
from pymongo.collation import Collation

load_dotenv()

database_id = os.getenv('ASTRA_DB_ID')
database_endpoint = os.getenv('ASTRA_API_ENDPOINT')
database_token = os.getenv('ASTRA_TOKEN')
database_token = os.getenv('ASTRA_DATACENTER_ID')

# connect to Astra DB
def connect_to_database():
    endpoint  = database_endpoint
    token = database_token
    if not token or not endpoint:
        raise RuntimeError(
            "Environment variables ASTRA_DB_API_ENDPOINT and ASTRA_DB_APPLICATION_TOKEN must be defined"
        )
    
    client = DataAPIClient(token)
    
    database = client.get_database(endpoint)
    
    print(f'Connected to database {database.info().name}')
    
    return database


# create a collection
def create_collection(database: Database, collection_name: str) -> Collection:    
    collection = database.create_collection(
        collection_name,
        metric=VectorMetric.COSINE,
        service=CollectionVectorServiceOptions(
            provider="nvidia",
            model_name="NV-Embed-QA",
        ),
    )

    print(f"Created collection {collection.full_name}")

    return collection


def upload_json_data(
    collection: Collection,
    data_file_path: str,
    embedding_string_creator: callable,
) -> None:
    # Read the JSON file and parse it into a JSON array.
    with open(data_file_path, "r", encoding="utf8") as file:
        json_data = json.load(file)

    # Add a $vectorize field to each piece of data. 
    documents = [
        {
            **data,
            "$vectorize": embedding_string_creator(data),
        }
        for data in json_data
    ]

    # Upload the data.
    inserted = collection.insert_many(documents)
    print(f"Inserted {len(inserted.inserted_ids)} items.")



if __name__ == '__main__':
    database = connect_to_database()
    
    # collection = create_collection(database, "test_collection")
    
    collection = database.get_collection('test_collection')
    
    # upload_json_data(
    #     collection,
    #     "sample_data.json", 
    #     lambda data: ( 
    #         f"summary: {data['summary']} | "
    #         f"genres: {', '.join(data['genres'])}"
    #     ),
    # )
    
    print("\nFinding books with rating greater than 4.7...")
    
    # Find documents that match a filter
    print("\nFinding books with rating greater than 4.7...")

    rating_cursor = collection.find({"rating": {"$gt": 4.7}})

    for document in rating_cursor:
        print(f"{document['title']} is rated {document['rating']}")

    # Perform a vector search to find the closest match to a search string
    print("\nUsing vector search to find a single scary novel...")

    single_vector_match = collection.find_one(
        {}, sort={"$vectorize": "A scary novel"}
    )

    print(f"{single_vector_match['title']} is a scary novel")

    # Combine a filter, vector search, and projection to find the 3 books with
    # more than 400 pages that are the closest matches to a search string,
    # and just return the title and author
    print("\nUsing filters and vector search to find 3 books with more than 400 pages that are set in the arctic, returning just the title and author...")

    vector_cursor = collection.find(
        {"numberOfPages": {"$gt": 400}},
        sort={"$vectorize": "A book set in the arctic"},
        limit=3,
        projection={"title": True, "author": True}
    )

    for document in vector_cursor:
        print(document)
    
    
