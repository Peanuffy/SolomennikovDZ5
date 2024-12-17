import pymongo
import pandas as pd
import json

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["depression_db"]
collection = db["depression_collection"]

file1_path = "Student Depression.csv"
file2_path = "Student Depression.json"

file1_data = pd.read_csv(file1_path).to_dict(orient="records")

with open(file2_path, "r") as json_file:
    file2_data = json.load(json_file)

all_data = file1_data + file2_data
collection.insert_many(all_data)


def convert_objectid_to_str(data):
    if isinstance(data, list):
        return [{**item, "_id": str(item["_id"])} for item in data]
    return {**data, "_id": str(data["_id"])}


query1 = list(collection.find().sort("Age", -1).limit(50))
query1 = convert_objectid_to_str(query1)
with open("query1.json", "w") as f:
    json.dump(query1, f, indent=4)

query2 = list(collection.find().sort("Academic Pressure", -1).limit(50))
query2 = convert_objectid_to_str(query2)
with open("query2.json", "w") as f:
    json.dump(query2, f, indent=4)

query3 = list(collection.find().sort("CGPA", -1).limit(100))
query3 = convert_objectid_to_str(query3)
with open("query3.json", "w") as f:
    json.dump(query3, f, indent=4)

query4 = list(collection.find({"Dietary Habits": "Healthy", "Sleep Duration": "5-6 hours"}).limit(50))
query4 = convert_objectid_to_str(query4)
with open("query4.json", "w") as f:
    json.dump(query4, f, indent=4)

query5 = collection.count_documents(
    {
        "$and": [
            {"City": "Visakhapatnam"},
            {"$or": [{"Age": {"$gt": 25, "$lte": 40}}]},
        ]
    }
)
with open("query5.json", "w") as f:
    json.dump(query5, f, indent=4)

Work_Study_Hours_stats = collection.aggregate(
    [
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$Work/Study Hours"},
                "avg_age": {"$avg": "$Work/Study Hours"},
                "max_age": {"$max": "$Work/Study Hours"},
            }
        }
    ]
)
Work_Study_Hours_stats = list(Work_Study_Hours_stats)
with open("Work_Study_Hours_stats.json", "w") as f:
    json.dump(Work_Study_Hours_stats, f, indent=4)

male_CGPA_stats = collection.aggregate(
    [
        {"$match": {"Gender": "Male"}},
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$CGPA"},
                "avg_age": {"$avg": "$CGPA"},
                "max_age": {"$max": "$CGPA"},
            }
        },
    ]
)
male_CGPA_stats = list(male_CGPA_stats)
with open("male_CGPA_stats.json", "w") as f:
    json.dump(male_CGPA_stats, f, indent=4)

female_CGPA_stats = collection.aggregate(
    [
        {"$match": {"Gender": "Female"}},
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$CGPA"},
                "avg_age": {"$avg": "$CGPA"},
                "max_age": {"$max": "$CGPA"},
            }
        },
    ]
)
female_CGPA_stats = list(female_CGPA_stats)
with open("female_CGPA_stats.json", "w") as f:
    json.dump(female_CGPA_stats, f, indent=4)

Job_Satisfaction_stats = collection.aggregate(
    [
        {
            "$group": {
                "_id": None,
                "min_purchases": {"$min": "$Job Satisfaction"},
                "avg_purchases": {"$avg": "$Job Satisfaction"},
                "max_purchases": {"$max": "$Job Satisfaction"},
            }
        }
    ]
)
Job_Satisfaction_stats = list(Job_Satisfaction_stats)
with open("Job_Satisfaction_stats.json", "w") as f:
    json.dump(Job_Satisfaction_stats, f, indent=4)

collection.delete_many({"Age": {"$gt": 30}})

collection.update_many({}, {"$inc": {"Age": 1}})

collection.update_many(
    {"Sleep Duration": "Less than 5 hours"}, {"$mul": {"CGPA": 1.15}}
)

collection.update_many({"Family History of Mental Illness": "No"}, {"$mul": {"Depression": 0.8}})

collection.update_many({"Profession": "Student"}, {"$mul": {"Depression": 2}})

collection.delete_many({"Depression": 0})

print("ОК")
