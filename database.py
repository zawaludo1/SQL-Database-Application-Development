#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''


def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y21s1c9120_yche3494"
    passwd = "510138903"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"

    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                user=userid,
                                password=passwd,
                                host=myHost)
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    # return the connection to use
    return conn


'''
Validate a sales agent login request based on username and password
'''


def checkUserCredentials(userName, password):
    # TODO - validate and get user info for a sales agent
    userInfo = ''
    try:
        conn = openConnection()
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    if conn is not None:
        try:
            curs = conn.cursor()
            curs.execute('''SELECT * FROM AGENT WHERE USERNAME = %(u)s AND PASSWORD = %(p)s''',
                         {'u': userName, 'p': password})
            userInfo = curs.fetchone()
        except psycopg2.Error as sqle:
            print("psycopg2.Error : " + sqle.pgerror)
        finally:
            curs.close()
            conn.close()
    return userInfo


'''
List all the associated bookings in the database for a given sales agent Id
'''


def findBookingsBySalesAgent(agentId):
    # TODO - list all the associated bookings in DB for a given sales agent Id
    booking_db = []
    conn = None
    try:
        conn = openConnection()
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    if (conn is not None):
        try:
            curs = conn.cursor()
            curs.execute("""SELECT B.BOOKING_NO, CONCAT(C.FIRSTNAME,' ', C.LASTNAME) AS CUSTOMER, B.PERFORMANCE, B.PERFORMANCE_DATE, 
            CONCAT(A.FIRSTNAME,' ', A.LASTNAME) AS AGENTNAME, B.INSTRUCTION 
            FROM AGENT AS A JOIN BOOKING AS B ON A.AGENTID = B. BOOKED_BY JOIN CUSTOMER AS C ON B.CUSTOMER = C.EMAIL AND A.AGENTID = %(agent_id)s 
            ORDER BY C.FIRSTNAME ASC, C.LASTNAME ASC""", {'agent_id': agentId})



            booking_db = curs.fetchall()
            booking_list = [{
                'booking_no': str(row[0]),
                'customer_name': row[1],
                'performance': row[2],
                'performance_date': row[3],
                'booked_by': row[4],
                'instruction': row[5]
            } for row in booking_db]
        except psycopg2.Error as sqle:
            print("psycopg2.Error : " + sqle.pgerror)
        finally:
            curs.close()
            conn.close()
    return booking_list



'''
Find a list of bookings based on the searchString provided as parameter
See assignment description for search specification
'''


def findBookingsByCustomerAgentPerformance(searchString):
    # TODO - find a list of bookings in DB based on searchString input
    booking_db = []
    conn = None
    try:
        conn = openConnection()
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    if (conn is not None):
        try:
            curs = conn.cursor()

            curs.execute("""SELECT B.BOOKING_NO, CONCAT(C.FIRSTNAME,' ', C.LASTNAME) AS CUSTOMER, B.PERFORMANCE ,B.PERFORMANCE_DATE,
            CONCAT(A.FIRSTNAME,' ', A.LASTNAME) AS AGENTNAME, B.INSTRUCTION
            FROM AGENT AS A  JOIN BOOKING AS B ON A.AGENTID = B. BOOKED_BY JOIN CUSTOMER AS C ON B.CUSTOMER = C.EMAIL 
            AND (LOWER(A.FIRSTNAME) LIKE %(ss)s OR LOWER(A.LASTNAME) LIKE %(ss)s OR LOWER(CONCAT(C.FIRSTNAME,' ', C.LASTNAME)) LIKE %(ss)s
            OR LOWER(PERFORMANCE) LIKE %(ss)s )ORDER BY C.FIRSTNAME ASC, C.LASTNAME ASC""", {'ss': '%' + searchString.lower() + '%'})
            booking_db = curs.fetchall()
            booking_list = [{
                'booking_no': str(row[0]),
                'customer_name': row[1],
                'performance': row[2],
                'performance_date': row[3],
                'booked_by': row[4],
                'instruction': row[5]
            } for row in booking_db]
        except psycopg2.Error as sqle:
            print("psycopg2.Error : " + sqle.pgerror)
        finally:
            curs.close()
            conn.close()

    # booking_db = [
    #     ['1', 'Bob Smith', 'The Lion King', '2021-06-05', 'Novak Djokovic', 'I\'d like to book 3 additional seats'],
    #     ['4', 'Peter Wood', 'The Lion King', '2021-06-05', 'Jeff Alexander', 'Can you please waitlist 4 seats?']
    # ]



    return booking_list


#####################################################################################
##  Booking (customer, performance, performance date, booking agent, instruction)  ##
#####################################################################################
'''
Add a new booking into the database - details for a new booking provided as parameters
'''


def addBooking(customer, performance, performance_date, booked_by, instruction):
    # TODO - add a booking
    # Insert a new booking into database
    # return False if adding was unsuccessful
    # return True if adding was successful
    conn = None
    try:
        conn = openConnection()
        curs = conn.cursor()
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    if (conn is not None):
        try:
            curs.callproc("aaa", [booked_by.lower()])
            output = curs.fetchone()
            result = output
            curs.execute("INSERT INTO BOOKING (CUSTOMER,PERFORMANCE,PERFORMANCE_DATE,BOOKED_BY,INSTRUCTION) "
                        + "values (%(a)s,%(b)s,%(c)s,%(d)s,%(e)s);",
                        {'a': customer, 'b': performance, 'c': performance_date, 'd': result, 'e': instruction})
            conn.commit()


        except psycopg2.Error as sqle:
            print("psycopg2.Error : " + sqle.pgerror)
            return False
        finally:
            curs.close()
            conn.close()


    return True


'''
Update an existing booking with the booking details provided in the parameters
'''


def updateBooking(booking_no, performance, performance_date, booked_by, instruction):
    # TODO - update an existing booking in DB
    # return False if updating was unsuccessful
    # return True if updating was successful
    conn = None
    row = ()
    try:
        conn = openConnection()
        curs = conn.cursor()
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    if (conn is not None):
        try:
            inf = [booking_no.lower(), performance, performance_date, booked_by, instruction]
            curs.callproc('bbb', inf)
            row = curs.fetchone()

            conn.commit()

        except psycopg2.Error as sqle:
            print("psycopg2.Error : " + sqle.pgerror)
            return False
        finally:
            curs.close()
            conn.close()

    return True
