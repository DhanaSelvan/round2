from elasticsearch import Elasticsearch, helpers
import pandas as pd

es = Elasticsearch("http://localhost:8989")
print(es.ping())

def createCollection(collectionName):
    if not es.indices.exists(index=collectionName):
        es.indices.create(index=collectionName)
        return "collection created successfully"
    else:
        return ("Collection is already available")

def indexData(collectionName, columnName):
    
    df = pd.read_csv("data.csv")
    columnList = df.columns
    
    if (columnName in columnList):
        df.drop(columns=[columnName], inplace=True)
    
    data = df.to_dict(orient = "records")
    
    actions = []
    for record in data:
        actions.append({"_index": collectionName,"_source": record})
    
    helpers.bulk(es, actions)

def searchByColumn(collectionName, columnName, columnValue):
    query = {
        "query": {
            "match": {
                columnName: columnValue
            }
        }
    }
    result = es.search(index=collectionName, body=query)
    return result['hits']['hits']
    
def getEmpCount(collectionName):
    return es.count(index=collectionName)['count']

def detEmpById(collectionName, empId):
    es.delete(index=collectionName, id=empId)

def getDepFacet(collectionName):
    query = {
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    result = es.search(index = collectionName, body=quesry)
    return result['aggregations']['department_count']['buckets']

collectionName = "Hash_Dhana"
print(createCollection(collectionName))

phoneCollection = "Hash_8428"
print(createCollection(phoneCollection))

print(getEmpCount(collectionName))

indexData(collectionName, "Department")
indexData(collectionName, "Gender")

print(getEmpCount(collectionName))

delEmpById(collectionName, "E02003")

print(getEmpCount(collectionName))

print(searchByColumn(collectionName, "Department", "IT"))
print(searchByColumn(collectionName, "Gender", "Male"))
print(searchByColumn(phoneCollection, "Department", "IT"))

print(getDepFacet(collectionName))
print(getDepFacet(phoneCollection))