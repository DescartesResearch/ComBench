import json

import requests

f = open("BenchmarkConfiguration.json")
data = json.load(f)
print(data)
r = requests.put("http://localhost:5000/startBenchmark", json=data)

# f = open("exampleConfiguration.json")
# data = json.load(f)
# print(data)
# r = requests.post("http://localhost:5000/config", json=data)


#f = open("exampleWorkload.json")
#data = json.load(f)
#print(data)
#r = requests.post("http://localhost:5000/workload", json=data)
