import mysql.connector
import sys
from constants import Constants

def importing(arguments, cursor):
    folderName = arguments[0]

def insertStudent(arguments, cursor):
    uciNetId, email, first, middle, last = arguments

def addEmail(arguments, cursor):
    uciNetId, email = arguments[0]

def deleteStudent(arguments, cursor):
    uciNetid = arguments[0]

def insertMachine(arguments, cursor):
    machineId, hostName, ipAddr, status = arguments

def insertUse(arguments, cursor):
    projId, uciNetId, machineId, start, end = arguments

def updateCourse(arguments, cursor):
    courseId, title = arguments

def listCourse(arguments, cursor):
    uciNetId = arguments[0]

def popularCourse(arguments, cursor):
    n = arguments[0]

def adminEmails(arguments, cursor):
    machineId = arguments[0]

def activeStudent(arguments, cursor):
    machineId, n, start, end = arguments

def machineUsage(arguments, cursor):
    courseId = arguments[0]

# Create a connection to the database
try:
    db_connection = mysql.connector.connect(user=Constants.USER, password=Constants.PASSWORD, database=Constants.DATABASE)

    #uncomment later for submission
    #db_connection = mysql.connector.connect(user='test', password='password', database='cs122a')

    cursor = db_connection.cursor()
    print("Successfully connected to the database")
    print("Initialization begin")

    #get function name
    functionName = sys.argv[1]
    arguments = sys.argv[2:]

    #call function depending on functionName

    #It's 3 AM rn I cant think of a better way to do this other than using a bunch of if statements 
    #Feel free to change it if you have a better way

    if (functionName == "import"):
        importing(arguments, cursor)
    elif (functionName == "insertStudent"):
        insertStudent(arguments, cursor)
    elif (functionName == "addEmail"):
        addEmail(arguments, cursor)
    elif (functionName == "deleteStudent"):
        deleteStudent(arguments, cursor)
    elif (functionName == "insertMachine"):
        insertMachine(arguments, cursor)
    elif (functionName == "insertUse"):
        insertUse(arguments, cursor)
    elif (functionName == "updateCourse"):
        updateCourse(arguments, cursor)
    elif (functionName == "listCourse"):
        listCourse(arguments, cursor)
    elif (functionName == "popularCourse"):
        popularCourse(arguments, cursor)
    elif (functionName == "adminEmails"):
        adminEmails(arguments, cursor)
    elif (functionName == "activeStudent"):
        activeStudent(arguments, cursor)
    elif (functionName == "machineUsage"):
        machineUsage(arguments, cursor)



    db_connection.commit()
    print("Initialization end successfully")

except mysql.connector.Error as error:
    print(f"Failed to execute SQL script: {error}")

finally:
    if db_connection.is_connected():
        cursor.close()
        db_connection.close()
        print("MySQL connection is closed")