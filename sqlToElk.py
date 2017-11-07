import json
import socket
from datetime import datetime, timedelta
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

logstashURL = ''

def main():
    query = '' #SQL query were the results will be sent to elasticsearch
    sqlConnection = '' #Takes a sqlConnection object that follows DB API outlined by python
    port = '' #this is the logstash port set up to receive data for the index
    paramaters = ('') #this is used if using paramaterized SQL queries for better security
    SQLtoLogstash(query, sqlConnection, port, paramaters

def SQLtoLogstash(query, connection, port, paramaters=""):
    count = 0

    # Gets SQL query
    cursor = connection.cursor()
    cursor.execute(query, paramaters)

    results = cursor.fetchone()  # Results from the query
    cols = [item[0] for item in cursor.description]  # gets colum names to help build our data structure

    # Parses SQL output to Logstash friendly input
    # Itterates through SQL results one at a time
    while results is not None:
        parsedResults = SQLParsing(results, cols)
        jsonResults = json.dumps(parsedResults)
        if PRINTTOCONSOLE:
            print jsonResults, "\n"

        # Sends to logstash through UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(jsonResults, (logstashURL, port))
        count += 1

        results = cursor.fetchone()

    print count, " SQL matches sent to Elastic Search"


def SQLParsing(results, cols):

    parsedResult = {}
    colCount = 0
    resultCount = 0

    for field in results:
        if isinstance(field, datetime):
            # if the time is greater than now, then they will keep registering when you search again
            if field > datetime.now():
                field = datetime.now()
            field = field.isoformat()

        parsedResult[cols[colCount]] = str(field)
        colCount += 1

    return parsedResult


if __name__ == "__main__":
    main()