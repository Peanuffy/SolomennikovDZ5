import json
import re
from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('mongodb://localhost:27017/')
db = client['data_db']
collection = db['data_collection']

def parse_text_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        raw_data = file.read()
    entries = raw_data.split('=====')
    parsed_data = []
    for entry in entries:
        fields = dict(re.findall(r'(\w+)::([^\n]+)', entry))
        if fields:
            fields['salary'] = int(fields['salary'])
            fields['age'] = int(fields['age'])
            fields['year'] = int(fields['year'])
            fields['id'] = int(fields['id'])
            parsed_data.append(fields)
    return parsed_data

text_data = parse_text_file('task_2_item.text')
collection.insert_many(text_data)

def serialize_objectid(obj):
    if isinstance(obj, dict):
        return {key: serialize_objectid(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_objectid(element) for element in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

pipeline_salary = [
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
]
salary_stats = list(collection.aggregate(pipeline_salary))
with open('query_salary_stats.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(salary_stats), file, ensure_ascii=False, indent=4)

pipeline_job_count = [
    {"$group": {"_id": "$job", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
job_counts = list(collection.aggregate(pipeline_job_count))
with open('query_job_counts.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(job_counts), file, ensure_ascii=False, indent=4)

pipeline_salary_city = [
    {"$group": {
        "_id": "$city",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }},
    {"$sort": {"_id": 1}}
]
salary_by_city = list(collection.aggregate(pipeline_salary_city))
with open('query_salary_by_city.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(salary_by_city), file, ensure_ascii=False, indent=4)

pipeline_salary_job = [
    {"$group": {
        "_id": "$job",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }},
    {"$sort": {"_id": 1}}
]
salary_by_job = list(collection.aggregate(pipeline_salary_job))
with open('query_salary_by_job.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(salary_by_job), file, ensure_ascii=False, indent=4)

pipeline_age_city = [
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"_id": 1}}
]
age_by_city = list(collection.aggregate(pipeline_age_city))
with open('query_age_by_city.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(age_by_city), file, ensure_ascii=False, indent=4)

pipeline_age_job = [
    {"$group": {
        "_id": "$job",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"_id": 1}}
]
age_by_job = list(collection.aggregate(pipeline_age_job))
with open('query_age_by_job.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(age_by_job), file, ensure_ascii=False, indent=4)

pipeline_max_salary_min_age = [
    {"$sort": {"age": 1, "salary": -1}},
    {"$limit": 1}
]
max_salary_min_age = list(collection.aggregate(pipeline_max_salary_min_age))
with open('query_max_salary_min_age.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(max_salary_min_age), file, ensure_ascii=False, indent=4)

pipeline_min_salary_max_age = [
    {"$sort": {"age": -1, "salary": 1}},
    {"$limit": 1}
]
min_salary_max_age = list(collection.aggregate(pipeline_min_salary_max_age))
with open('query_min_salary_max_age.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(min_salary_max_age), file, ensure_ascii=False, indent=4)

pipeline_age_salary_city = [
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"avg_age": -1}}
]
age_salary_by_city = list(collection.aggregate(pipeline_age_salary_city))
with open('query_age_salary_by_city.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(age_salary_by_city), file, ensure_ascii=False, indent=4)

pipeline_salary_ranges = [
    {"$match": {
        "$or": [
            {"age": {"$gt": 18, "$lt": 25}},
            {"age": {"$gt": 50, "$lt": 65}}
        ]
    }},
    {"$group": {
        "_id": {"city": "$city", "job": "$job"},
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }},
    {"$sort": {"_id.city": 1, "_id.job": 1}}
]
salary_ranges = list(collection.aggregate(pipeline_salary_ranges))
with open('query_salary_ranges.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(salary_ranges), file, ensure_ascii=False, indent=4)

pipeline_custom = [
    {"$match": {"city": "Тбилиси"}},
    {"$group": {
        "_id": "$job",
        "total_salary": {"$sum": "$salary"}
    }},
    {"$sort": {"total_salary": -1}}
]
custom_query = list(collection.aggregate(pipeline_custom))
with open('query_custom.json', 'w', encoding='utf-8') as file:
    json.dump(serialize_objectid(custom_query), file, ensure_ascii=False, indent=4)

