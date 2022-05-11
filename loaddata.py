import pyodbc
import datetime
from connect_db import connect_db

def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    # Creating RentalPlan table
    conn.execute("CREATE TABLE RentalPlan(pid INT PRIMARY KEY, pname VARCHAR(50), monthly_fee FLOAT, max_movies INT)")
    
    # Opening filename
    openFile = open(filename, 'r')
    line = openFile.readline().strip()
    
    # Reading filename
    while(line):

        # Tokenize each line by '|'
        lineTokens = line.split("|")
        pid = lineTokens[0]
        pname = lineTokens[1]
        monthly_fee = lineTokens[2]
        max_movies = lineTokens[3]
        
        # Insert the values into RentalPlan table
        conn.execute("INSERT into RentalPlan(pid, pname, monthly_fee, max_movies)\nVALUES(" + pid + ",'" + pname + "'," + monthly_fee + "," + max_movies + ")")
        
        # Get the next line
        line = openFile.readline().strip()
        
    openFile.close()
    
def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    
    # Creating Customer table
    conn.execute("CREATE TABLE Customer(cid INT PRIMARY KEY, pid INT REFERENCES RentalPlan(pid) ON DELETE CASCADE, username VARCHAR(50), password VARCHAR(50))")
    
    # Opening filename
    openFile = open(filename, 'r')
    line = openFile.readline().strip()
    
    # Reading Customer.txt line by line
    while(line):

        # Tokenize each line by '|'
        lineTokens = line.split("|")
        cid = lineTokens[0]
        pid = lineTokens[1]
        username = lineTokens[2]
        password = lineTokens[3]
        
        # Insert the values into Customer table
        conn.execute("INSERT into Customer(cid, pid, username, password)\nVALUES(" + cid + "," + pid + ",'" + username + "','" + password + "')")
        
        # Get the next line
        line = openFile.readline().strip()
    
    openFile.close()

def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    # Creating Movie table
    conn.execute("CREATE TABLE Movie(mid INT PRIMARY KEY, mname VARCHAR(50), year INT)")
    
    # Opening filename
    openFile = open(filename, 'r')
    line = openFile.readline().strip()
    
    # Reading filename line by line
    while(line):

        # Tokenize each line by '|'
        lineTokens = line.split("|")
        mid = lineTokens[0]
        mname = lineTokens[1]
        year = lineTokens[2]
        
        # Insert the values into Movie table
        conn.execute("INSERT into Movie(mid, mname, year)\nVALUES(" + mid + ",'" + mname + "'," + year + ")")
        
        # Get the next line
        line = openFile.readline().strip()
    
    openFile.close()

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    # Creating Rental table
    conn.execute("CREATE TABLE Rental(cid INT REFERENCES Customer(cid) ON DELETE CASCADE, mid INT REFERENCES Movie(mid) ON DELETE CASCADE, date_and_time DATETIME, status VARCHAR(6))")
    
    # Opening filename
    openFile = open(filename, 'r')
    line = openFile.readline().strip()
    
    # Reading filename line by line
    while(line):

        # Tokenize each line by '|'
        lineTokens = line.split("|")
        cid = lineTokens[0]
        mid = lineTokens[1]
        date_and_time = lineTokens[2]
        status = lineTokens[3]
        
        # Insert the values into Rental table
        conn.execute("INSERT into Rental(cid, mid, date_and_time, status)\nVALUES(" + cid + "," + mid + ",'" + date_and_time + "','" + status + "')")
        
        # Get the next line
        line = openFile.readline().strip()
    
    openFile.close()
    
def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)
    
    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






