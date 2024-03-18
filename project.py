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

    get_num_users_command = '''
        SELECT COUNT(*)
        FROM users;
    '''
    get_num_machines_command = '''
        SELECT COUNT(*)
        FROM machines;
    '''
    get_num_courses_command = '''
        SELECT COUNT(*)
        FROM courses
    '''


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
            #print(f'Executing: {query}')
    
    cursor.execute(get_num_users_command)
    result = cursor.fetchone()
    num_users = result[0]
    cursor.execute(get_num_machines_command)
    result = cursor.fetchone()
    num_machines = result[0]
    cursor.execute(get_num_courses_command)
    result = cursor.fetchone()
    num_courses = result[0]

    return f"{num_users},{num_machines},{num_courses}"


def insertStudent(arguments, cursor):
    uciNetId, email, first, middle, last = arguments

    insert_user = """
    INSERT INTO users (UCINetID, FirstName, MiddleName, LastName)
    VALUES (%s, %s, %s, %s)
    """
    insert_student = "INSERT INTO students (UCINetID) VALUES (%s)"

    try:
        cursor.execute(insert_user, (uciNetId, first, middle, last))
        cursor.execute(insert_student, (uciNetId,))
        
        print("Success")
    except Exception as e:
        print("Fail")


def addEmail(arguments, cursor):
    uciNetId, email = arguments

    add_email = "UPDATE users SET Email = %s WHERE UCINetID = %s"

    try:
        cursor.execute(add_email, (email, uciNetId))
        
        print("Success")
    except Exception as e:
        print("Fail")

def deleteStudent(arguments, cursor):
    uciNetid = arguments[0]
    if (uciNetid == "NULL"):
        uciNetid = None
    deleteStudentCommand = '''
        DELETE 
        FROM users
        WHERE UCINetID = %s;
    '''
    try:
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

    insertMachineCommand = ''' 
        INSERT INTO machines (MachineId, HostName, IPAddress, OperationalStatus, Location)
        VALUES (%s, %s, %s, %s, %s);
        '''
    try:

        cursor.execute(insertMachineCommand, (machineId,hostName,ipAddr,status,location))
        return True
    except Exception as e:
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

    insertUseCommand = ''' 
        INSERT INTO use1 (ProjectID, StudentUCINetID, MachineID, StartDate, EndDate)
        VALUES (%s, %s, %s, %s, %s);
        '''
    try:

        cursor.execute(insertUseCommand, (projId, uciNetId, machineId, start, end))
        return True
    except Exception as e:
        return False

def updateCourse(arguments, cursor):
    courseId, title = arguments

def listCourse(arguments, cursor):
    uciNetId = arguments[0]

def popularCourse(arguments, cursor):
    n = arguments[0]

def adminEmails(arguments, cursor):
    machineId = int(arguments[0])
    adminEmailCommand = '''
        SELECT A.UCINetID
        FROM admins A
        JOIN manage M ON A.UCINetID = M.UCINetID
        WHERE M.MachineID = %s
        ORDER BY A.UCINetID asc;
    '''
    try:
        cursor.execute(adminEmailCommand, (machineId,))
        emails = []
        for id in cursor:
            emails.append(id + '@uci.edu')
        emailList = (';'.join(emails))
        return emailList
    except:
        return ''
    # change to return empty table? 

def activeStudent(arguments, cursor):
    machineId, n, start, end = arguments
    activeStudentCommand = '''
        SELECT S.UCINetID
        FROM students S
        JOIN use1 U ON S.UCINetID = U.UCINetID
        WHERE U.MachineID = %s AND %s >= U.StartDate AND %s <= U.EndDate
    '''
    # need to add n count and output in a table

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
        print(output)
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
