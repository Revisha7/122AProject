import mysql.connector
import sys
from constants import Constants
import os
import csv

def importing(arguments, cursor):
    folderName = arguments[0]

    table_deletion_commands = [
        'DROP TABLE IF EXISTS manage CASCADE;',
        'DROP TABLE IF EXISTS use1 CASCADE;',
        'DROP TABLE IF EXISTS students CASCADE;',
        'DROP TABLE IF EXISTS projects CASCADE;',
        'DROP TABLE IF EXISTS machines CASCADE;',
        'DROP TABLE IF EXISTS emails CASCADE;',
        'DROP TABLE IF EXISTS courses CASCADE;',
        'DROP TABLE IF EXISTS admins CASCADE;',
        'DROP TABLE IF EXISTS users CASCADE;'
    ]
    table_creation_commands = [
        '''CREATE TABLE users (
            UCINetID VARCHAR(20) PRIMARY KEY NOT NULL,
            FirstName VARCHAR(50),
            MiddleName VARCHAR(50),
            LastName VARCHAR(50)
        )''',
        
        '''CREATE TABLE admins (
            UCINetID VARCHAR(20) PRIMARY KEY NOT NULL,
            FOREIGN KEY (UCINetID) REFERENCES users (UCINetID) ON DELETE CASCADE
        )''',
        
        '''CREATE TABLE courses (
            CourseID INT PRIMARY KEY NOT NULL,
            Title VARCHAR(200),
            Quarter VARCHAR(20)
        )''',
        
        '''CREATE TABLE emails (
            UCINetID VARCHAR(20),
            Email    VARCHAR(512),
            PRIMARY KEY (UCINetID, Email),
            FOREIGN KEY (UCINetID) REFERENCES users (UCINetID) ON DELETE CASCADE
        )''',
        
        '''CREATE TABLE machines (
            MachineID INT PRIMARY KEY NOT NULL,
            HostName VARCHAR(50),
            IPAddress VARCHAR(15),
            OperationalStatus VARCHAR(50),
            Location VARCHAR(255)
        )''',
        
        '''CREATE TABLE manage (
            AdministratorUCINetID VARCHAR(20),
            MachineID INT,
            PRIMARY KEY (AdministratorUCINetID, MachineID),
            FOREIGN KEY (AdministratorUCINetID) REFERENCES admins(UCINetID),
            FOREIGN KEY (MachineID) REFERENCES machines(MachineID)
        )''',
        
        '''CREATE TABLE projects (
            ProjectID INT PRIMARY KEY NOT NULL,
            Name VARCHAR(100),
            Description TEXT,
            CourseID INT NOT NULL,
            FOREIGN KEY (CourseID) REFERENCES courses(CourseID)
        )''',
        
        '''CREATE TABLE students (
            UCINetID VARCHAR(20) PRIMARY KEY NOT NULL,
            FOREIGN KEY (UCINetID) REFERENCES users(UCINetID) ON DELETE CASCADE
        )''',
        
        '''CREATE TABLE use1 (
            ProjectID INT,
            StudentUCINetID VARCHAR(20),
            MachineID INT,
            StartDate DATE,
            EndDate DATE,
            PRIMARY KEY (ProjectID, StudentUCINetID, MachineID),
            FOREIGN KEY (ProjectID) REFERENCES projects(ProjectID),
            FOREIGN KEY (StudentUCINetID) REFERENCES students(UCINetID),
            FOREIGN KEY (MachineID) REFERENCES machines(MachineID)
        )''',
    ]

    # Execute deletion of tables
    for command in table_deletion_commands:
        cursor.execute(command)

    # Execute creation of tables
    for command in table_creation_commands:
        cursor.execute(command)
       
         
    
    import_order = ['users.csv', 'admins.csv', 'courses.csv', 'machines.csv', 'projects.csv', 'students.csv', 'use.csv', 'manage.csv', 'emails.csv']

    for file in import_order:
        with open(os.path.join(folderName, file), 'r') as csvfile:
            reader = csv.reader(csvfile)
            if file == 'use.csv':
                table_name = 'use1'
            else:
                table_name = file[:-4]
            all_values = []
            for row in reader:
                # Replace 'NULL' with None for SQL NULL
                processed_row = ["''" if value == 'NULL' else f"'{value}'" for value in row]
                all_values.append(f"({','.join(processed_row)})")

            values_string = ',\n'.join(all_values)
            query = f"INSERT INTO `{table_name}` VALUES\n{values_string};"
            cursor.execute(query)
            print(f'Executing: {query}')
                



def insertStudent(arguments, cursor):
    uciNetId, email, first, middle, last = arguments

def addEmail(arguments, cursor):
    uciNetId, email = arguments[0]

def deleteStudent(arguments, cursor):
    uciNetid = arguments[0]
    if (uciNetid == "NULL"):
        uciNetid = None
    checkStudentExistsCommand = '''
        SELECT * 
        FROM students
        WHERE UCINetID = %s;
    '''
    deleteStudentCommand = '''
        DELETE 
        FROM users
        WHERE UCINetID = %s;
    '''
    try:
        cursor.execute(checkStudentExistsCommand, (uciNetid,))
        
        exists = 0
        for student in cursor:
            exists += 1

        #If student doesn't exist, return False
        if exists == 0:
            return False

        cursor.execute(deleteStudentCommand, (uciNetid,))
        return True
    except:
        return False

def insertMachine(arguments, cursor):
    machineId, hostName, ipAddr, status, location = arguments
    for argument in [machineId,hostName,ipAddr,status,location]:
        if argument == "NULL":
            argument = None
    if machineId != None:
        machineId = int(machineId)

    checkMachineExistsCommand = '''
        SELECT * 
        FROM machines
        WHERE MachineId = %s;
    '''

    insertMachineCommand = ''' 
        INSERT INTO machines (MachineId, HostName, IPAddress, OperationalStatus, Location)
        VALUES (%s, %s, %s, %s, %s);
        '''
    try:
        cursor.execute(checkMachineExistsCommand, (machineId,))

        exists = 0
        for machine in cursor:
            exists += 1

        #if machine with same id already exists, return false
        if exists > 0:
            return False

        cursor.execute(insertMachineCommand, (machineId,hostName,ipAddr,status,location))
        return True
    except:
        return False

def insertUse(arguments, cursor):
    projId, uciNetId, machineId, start, end = arguments

    for argument in [projId, uciNetId, machineId, start, end]:
        if argument == "NULL":
            argument = None
    if machineId != None:
        machineId = int(machineId)
    if projId != None:
        projId = int(projId)

    checkProjectExistsCommand = '''
        SELECT * 
        FROM projects
        WHERE ProjectID = %s
    '''

    checkStudentExistsCommand = '''
        SELECT * 
        FROM students
        WHERE UCINetID = %s
    '''

    checkMachineExistsCommand = '''
        SELECT * 
        FROM machines
        WHERE MachineID = %s
    '''

    checkUseExistsCommand = '''
        SELECT * 
        FROM use1
        WHERE ProjectID = %s AND StudentUCINetID = %s AND MachineID = %s;
    '''

    insertUseCommand = ''' 
        INSERT INTO use1 (ProjectID, StudentUCINetID, MachineID, StartDate, EndDate)
        VALUES (%s, %s, %s, %s, %s);
        '''
    try:
        exists = 0
        cursor.execute(checkProjectExistsCommand, (projId,))
        for use in cursor:
            exists += 1
        cursor.execute(checkStudentExistsCommand, (uciNetId,))
        for use in cursor:
            exists += 1
        cursor.execute(checkMachineExistsCommand, (machineId,))
        for use in cursor:
            exists += 1

        #if project/student/machine doesn't exist
        if exists < 3:
            return False



        cursor.execute(checkUseExistsCommand, (projId, uciNetId, machineId))

        exists = 0
        for use in cursor:
            exists += 1

        #if use with same primary key already exists, return false
        if exists > 0:
            return False

        cursor.execute(insertUseCommand, (projId, uciNetId, machineId, start, end))
        return True
    except Exception as e:
        print(e)
        return False



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
    #print("Successfully connected to the database")
    #print("Initialization begin")

    #get function name
    functionName = sys.argv[1]
    arguments = sys.argv[2:]

    #call function depending on functionName, and print returned output

    if (functionName == "import"):
        output = importing(arguments, cursor)
    elif (functionName == "insertStudent"):
        output = insertStudent(arguments, cursor)
    elif (functionName == "addEmail"):
        output = addEmail(arguments, cursor)
    elif (functionName == "deleteStudent"):
        output = deleteStudent(arguments, cursor)
        if (output):
            print("Success")
        else:
            print("Fail")
    elif (functionName == "insertMachine"):
        output = insertMachine(arguments, cursor)
        if (output):
            print("Success")
        else:
            print("Fail")
    elif (functionName == "insertUse"):
        output = insertUse(arguments, cursor)
        if (output):
            print("Success")
        else:
            print("Fail")
    elif (functionName == "updateCourse"):
        output = updateCourse(arguments, cursor)
    elif (functionName == "listCourse"):
        output = listCourse(arguments, cursor)
    elif (functionName == "popularCourse"):
        output = popularCourse(arguments, cursor)
    elif (functionName == "adminEmails"):
        output = adminEmails(arguments, cursor)
    elif (functionName == "activeStudent"):
        output = activeStudent(arguments, cursor)
    elif (functionName == "machineUsage"):
        output = machineUsage(arguments, cursor)



    db_connection.commit()


    #print("Initialization end successfully")

except mysql.connector.Error as error:
    #will delete before submission
    print(f"Failed to execute SQL script: {error}")

finally:
    if db_connection.is_connected():
        cursor.close()
        db_connection.close()

        #print("MySQL connection is closed")