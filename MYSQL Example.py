import Element451API as E451
import mysql.connector

# Used for loading environmental variables.
import os
from dotenv import load_dotenv

load_dotenv()

my_client = os.getenv("my_client")
my_api = os.getenv("my_api")
my_feature = os.getenv("my_feature")
my_segment = os.getenv("my_segment")
my_template = os.getenv("my_template_guid")

# Get Element Data
elementdata = E451.api_data_request(
    my_client,
    my_api,
    my_feature,
    my_segment,
    my_template,
    column_key="slug",
)
# Connect to mysql database.
mydb = mysql.connector.connect(
    host=os.getenv("host"),
    port=os.getenv("port"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    database=os.getenv("database"),
)

mycursor = mydb.cursor()
val = []
for x in elementdata["data"]:
    studentID = x["user-elementid"]
    studentName = x["user-first-name"]
    studentTerm = x["user-education-term"]
    studentMajor = x["user-education-prefered-major"]
    mytuple = (studentID, studentName, studentTerm, studentMajor)
    val.append(mytuple)


sql = (
    "INSERT INTO training_data (Student_ID, First, Term, Major) VALUES (%s, %s, %s, %s)"
)
mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount, "records inserted")

mycursor.close()
mydb.close()
