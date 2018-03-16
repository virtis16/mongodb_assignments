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
query = ("Select  p.Pnumber, p.Pname, d.Dname, w.Essn, e.Lname, e.Fname, w.Hours from project as p, department as d, works_on as w, employee as e Where d.Dnumber = p.Dnum AND w.Pno = p.Pnumber AND e.Ssn = w.Essn order by p.Pnumber;")


cursor.execute(query)
obj_arr = []
arr = []
oldPnumber = 0;
for (Pnumber, Pname, Dname, Essn, Lname, Fname, Hours) in cursor:
  print(Pnumber)
  subobj = {}
  subobj['Lname'] = Lname
  subobj['Fname'] = Fname
  subobj['Hours'] = Hours
  if(oldPnumber != Pnumber):
  
	  if(len(arr)>0):
		obj['employees'] = arr
		obj_arr.append(obj)
		print(obj)
	  obj = {}
	  arr = []  
	  obj['Pnumber'] = Pnumber
	  obj['Pname'] = Pname
	  obj['Dname'] = Dname
	  arr.append(subobj)
	  oldPnumber = Pnumber;
	  #print(x)
  else:
	  arr.append(subobj)
	  
obj['employees'] = arr
obj_arr.append(obj)
print(obj)  
  

my_json_string = json.dumps(obj_arr)
print(my_json_string)
cursor.close()  
cnx.close()
print("Importing into MongoDB")
print("----------------------")
result = db.project.insert_many(obj_arr)
print(result.inserted_ids)