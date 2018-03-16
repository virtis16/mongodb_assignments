import mysql.connector
import json
import pymongo
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client.office

cnx = mysql.connector.connect(user='root', password='password',
                              host='127.0.0.1',
                              database='office')
cursor = cnx.cursor()

print("Getting required data from MySQL")
print("--------------------------------")
query = ("SELECT d.Dname, e.Lname, l.Dlocation FROM office.department as d LEFT OUTER JOIN office.employee as e on d.Mgr_ssn = e.Ssn LEFT OUTER JOIN office.dept_locations as l on d.Dnumber = l.Dnumber order by d.Dname;")


cursor.execute(query)
obj_arr = []
arr = []
oldDname = "";
for (Dname, Lname, Dlocation) in cursor:
  subobj = {}
  subobj['Dlocation'] = Dlocation
  if(oldDname != Dname):
  
	  if(len(arr)>0):
		obj['locations'] = arr
		obj_arr.append(obj)
		print(obj)
	  obj = {"locations": []}
	  arr = []
	  obj['Dname'] = Dname
	  obj['Lname'] = Lname
	  arr.append(subobj)
	  oldDname = Dname;
	  #print(x)
  else:
	  arr.append(subobj)
	  
	  
obj['locations'] = arr
obj_arr.append(obj)
print(obj) 
  

my_json_string = json.dumps(obj_arr)
print(my_json_string)
cursor.close()  
cnx.close()

print("Importing into MongoDB")
print("----------------------")
result = db.department.insert_many(obj_arr)
print(result.inserted_ids)