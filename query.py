from flask import Flask, g, request, jsonify
import pyodbc
from connect_db import connect_db
import sys
import time, datetime


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'azure_db'):
        g.azure_db = connect_db()
        g.azure_db.autocommit = True
        g.azure_db.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
    return g.azure_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'azure_db'):
        g.azure_db.close()



@app.route('/login')
def login():
    username = request.args.get('username', "")
    password = request.args.get('password', "")
    cid = -1
    #print (username, password)
    conn = get_db()
    #print (conn)
    cursor = conn.execute("SELECT * FROM Customer WHERE username = ? AND password = ?", (username, password))
    records = cursor.fetchall()
    #print records
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)




@app.route('/getRenterID')
def getRenterID():
    """
       This HTTP method takes mid as input, and
       returns cid which represents the customer who is renting the movie.
       If this movie is not being rented by anyone, return cid = -1
    """
    mid = int(request.args.get('mid', -1))

    # WRITE YOUR CODE HERE
    mid = request.args.get('mid', "")
    cid = -1    # First assume movie is not rented

    # Check if movie id is in the Rental and if it is being rented
    conn = get_db()
    cursor = conn.execute("SELECT * FROM Rental WHERE mid = ? AND status = 'open'", (mid))
    records = cursor.fetchall()
  
    # If not in the system
    if len(records) != 0:
        cid = records[0][0]


    response = {'cid': cid}
    return response

# Helper function for Task 4 and Task 5
def findingRemainingRentals(conn, cid):
    # Getting current number of movies rented
    cursor = conn.execute("SELECT COUNT(*) FROM Customer C, Rental R WHERE C.cid = R.cid and C.cid = ? and status = 'open'", (cid))
    numOfMoviesRented = cursor.fetchall()
    
    # Getting the maximum number of movies that cid can rent
    cursor = conn.execute("SELECT P.max_movies FROM RentalPlan P, Customer C WHERE P.pid = C.pid and C.cid = ?", (cid))
    maxNumOfRentedMovies = cursor.fetchall()
    
    # Finding out remaining movies cid can rent
    if (len(numOfMoviesRented) != 0 and len(maxNumOfRentedMovies) != 0):
        return (maxNumOfRentedMovies[0][0] - numOfMoviesRented[0][0])
    
    # If customer currently rented no movies
    if (len(maxNumOfRentedMovies) != 0):
        return maxNumOfRentedMovies[0][0]
    
    # If customer is not allowed to rent movies
    return -1

@app.route('/getRemainingRentals')
def getRemainingRentals():
    """
        This HTTP method takes cid as input, and returns n which represents
        how many more movies that cid can rent.

        n = 0 means the customer has reached its maximum number of rentals.
    """
    cid = int(request.args.get('cid', -1))


    # Tell ODBC that you are starting a multi-statement transaction
    conn = get_db()
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    n = findingRemainingRentals(conn, cid)
    
    conn.autocommit = True

    response = {"remain": n}        
    return jsonify(response)





def currentTime():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@app.route('/rent')
def rent():
    """
        This HTTP method takes cid and mid as input, and returns either "success" or "fail".

        It returns "fail" if C1, C2, or both are violated:
            C1. at any time a movie can be rented to at most one customer.
            C2. at any time a customer can have at most as many movies rented as his/her plan allows.
        Otherwise, it returns "success" and also updates the database accordingly.
    """
    cid = int(request.args.get('cid', -1))
    mid = int(request.args.get('mid', -1))

    conn = get_db()

     # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    response = {"rent": "fail"}
    
    # Assume customer can rent the movie
    conn.execute("INSERT into Rental(cid, mid, date_and_time, status) VALUES (?, ?, ?, 'open')", (cid, mid, currentTime()))
    
    # Checking if C1 (condition 1) was violated
    cursor = conn.execute("SELECT * FROM Rental WHERE cid != ? and mid = ? and status = 'open'", (cid, mid))
    records = cursor.fetchall()
    if (len(records) != 0): # If a customer is already renting that movie
        conn.rollback()
        return jsonify(response)
    
    # Checking if C2 (condiiton 2) was violated
    remainingMovieRentals = findingRemainingRentals(conn, cid)
    
    if (remainingMovieRentals < 0): # If customer goes over the limit of how many movies you can rent
        conn.rollback()
        return jsonify(response)
    
    conn.autocommit = True
    response = {"rent": "success"}
    return jsonify(response)

