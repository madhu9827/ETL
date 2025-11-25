import requests,csv,json,mysql.connector,pymongo
from pydantic import BaseModel
from pymongo import MongoClient
fp=open('cart.json','w')
fp1=open('cart.csv','w',newline='')
cart_resp=requests.get('https://dummyjson.com/carts')
carts_data=cart_resp.json()
# print(carts_data)
cart_csv=[]
cart_json=[]
for carts in carts_data["carts"]:
    cart_csv.append((carts["id"],
                      json.dumps(carts["products"]),
                      carts["total"],
                      carts["discountedTotal"],
                      carts["totalProducts"],
                      carts["totalQuantity"]
                    ))
    cart_json.append({"id":carts["id"],
                       "products":carts["products"],
                       "total":carts["total"],
                       "discountedTotal":carts["discountedTotal"],
                       "total_Products":carts["totalProducts"],
                       "total_Quantity":carts["totalQuantity"]
                    })
# print(cart_json)
json.dump(cart_json,fp)
print("json file created")

cw=csv.writer(fp1)
cw.writerow(["id","products","total","discountedTotal","totalProducts","totalQunatity"])
cw.writerows(cart_csv)
print("csv file created")
# mongodb
client = MongoClient('mongodb://localhost:27017/')
db = client['db3']
collection = db['cart_collection']

collection.insert_many(cart_json)
print("Data inserted into MongoDB successfully!")

# mysql
dbcon = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="taskdb"
)
cursor=dbcon.cursor()
st_sql='''
insert into task_two Values(%s,%s,%s,%s,%s,%s)'''
cursor.executemany(st_sql,cart_csv)
dbcon.commit()
print("sql file is created")