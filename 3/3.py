import csv
import json
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['data_db']
collection = db['data_collection']

csv_file = 'task_3_item.csv'
with open(csv_file, 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file, delimiter=';')
    csv_data = [
        {
            "job": row["job"],
            "salary": int(row["salary"]),
            "id": int(row["id"]),
            "city": row["city"],
            "year": int(row["year"]),
            "age": int(row["age"]),
        }
        for row in csv_reader
    ]

collection.insert_many(csv_data)

collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})

collection.update_many({}, {"$inc": {"age": 1}})

selected_jobs = ["Учитель", "Врач"]
collection.update_many(
    {"job": {"$in": selected_jobs}},
    {"$mul": {"salary": 1.05}}
)

selected_cities = ["Астана", "Вильявисиоса"]
collection.update_many(
    {"city": {"$in": selected_cities}},
    {"$mul": {"salary": 1.07}}
)

complex_predicate = {
    "$and": [
        {"city": "Камбадос"},
        {"job": {"$in": ["Водитель", "Косметолог"]}},
        {"age": {"$gte": 40, "$lte": 60}}
    ]
}
collection.update_many(complex_predicate, {"$mul": {"salary": 1.10}})

random_predicate = {"year": {"$lt": 2005}}
collection.delete_many(random_predicate)

output_file = 'result_data.json'
result_data = list(collection.find({}, {"_id": 0}))
with open(output_file, 'w', encoding='utf-8') as file:
    json.dump(result_data, file, ensure_ascii=False, indent=4)
