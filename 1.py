import pickle
import json
from pymongo import MongoClient
from bson import ObjectId

with open('task_1_item.pkl', 'rb') as file:
    data = pickle.load(file)

client = MongoClient('mongodb://localhost:27017/')
db = client['data_db']
collection = db['data_collection']

collection.delete_many({})
collection.insert_many(data)

def serialize_objectid(obj):
    if isinstance(obj, dict):
        return {key: serialize_objectid(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_objectid(element) for element in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

query_1 = list(collection.find().sort('salary', -1).limit(10))
query_1 = serialize_objectid(query_1)
with open('query_1.json', 'w', encoding='utf-8') as file:
    json.dump(query_1, file, ensure_ascii=False, indent=4)

query_2 = list(collection.find({'age': {'$lt': 30}}).sort('salary', -1).limit(15))
query_2 = serialize_objectid(query_2)
with open('query_2.json', 'w', encoding='utf-8') as file:
    json.dump(query_2, file, ensure_ascii=False, indent=4)

city_filter = 'Трухильо'
job_filter = ['Архитектор', 'Учитель', 'Менеджер']
query_3 = list(collection.find({
    'city': city_filter,
    'job': {'$in': job_filter}
}).sort('age', 1).limit(10))
query_3 = serialize_objectid(query_3)
with open('query_3.json', 'w', encoding='utf-8') as file:
    json.dump(query_3, file, ensure_ascii=False, indent=4)

age_range = {'$gte': 25, '$lte': 35}
year_range = {'$gte': 2019, '$lte': 2022}
salary_filter = {
    '$or': [
        {'salary': {'$gt': 50000, '$lte': 75000}},
        {'salary': {'$gt': 125000, '$lt': 150000}}
    ]
}
query_4_count = collection.count_documents({
    'age': age_range,
    'year': year_range,
    **salary_filter
})
with open('query_4.json', 'w', encoding='utf-8') as file:
    json.dump({'count': query_4_count}, file, ensure_ascii=False, indent=4)
