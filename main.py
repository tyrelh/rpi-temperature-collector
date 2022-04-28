import sys
import time
import uuid
from datetime import datetime, timedelta
import boto3
import json
from decimal import Decimal
from random import randint
from w1thermsensor import W1ThermSensor, Unit
# from w1thermsensor import W1ThermSensor

FLAG_ADMIN = "-a"
FLAG_OFFSET = "-o"
FLAG_PERIOD = "-p"

VALUE_COLUMN = "value"
LOCATION_COLUMN = "location"
TIME_COLUMN = "time"
UUID_COLUMN = "uuid"
TABLE_PREFIX = "rpi-temperature-"
TEMPERATURE_UNIT = "°C"
TEST_TABLE_NAME = "rpiTempTestTable"

GET_REQUEST_WAIT_TIME = 0.1
UPDATE_REQUEST_WAIT_TIME = 5.5
READABLE_WAIT_TIME = 0

ADMIN = False
PERIOD = 1

dynamodb = boto3.resource('dynamodb')
sensor = W1ThermSensor()
# sensor1 = W1ThermSensor


def wait(seconds):
    if seconds > 0:
        time.sleep(seconds)


def getSensorReading():
    print("Reading temperature...", end="    ")
    wait(READABLE_WAIT_TIME)
    currentTemperature = round(sensor.get_temperature(Unit.DEGREES_C), 1)
    print(f"{currentTemperature}{TEMPERATURE_UNIT}.")
    wait(READABLE_WAIT_TIME)
    return currentTemperature


def getDateTime(now):
    return {
        "ms": int(now.timestamp() * 1000),
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "source": now
    }


def checkIfCurrentTableExists(tableName):
    tableCreationDate = False
    try:
        tableCreationDate = dynamodb.Table(tableName).creation_date_time
        wait(GET_REQUEST_WAIT_TIME)
    except:
        tableCreationDate = False

    return not not tableCreationDate


def createNewTable(tableName):
    print(f"Creating new table {tableName}...", end="    ")
    table = dynamodb.create_table(
        TableName=tableName,
        KeySchema=[
            {
                "AttributeName": LOCATION_COLUMN,
                "KeyType": "HASH"
            },
            {
                "AttributeName": TIME_COLUMN,
                "KeyType": "RANGE"
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": LOCATION_COLUMN,
                "AttributeType": "S"
            },
            {
                "AttributeName": TIME_COLUMN,
                "AttributeType": "N"
            }
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 3,
            "WriteCapacityUnits": 2
        }
    )
    wait(UPDATE_REQUEST_WAIT_TIME) # creating a new table takes time
    if table:
        print("Complete.")
        wait(READABLE_WAIT_TIME)


def pushStat(stat, id, dateTime, tableName):
    print(f"Pushing value {stat} for location {id} at time {dateTime['time']} to table {tableName}...", end="    ")
    wait(READABLE_WAIT_TIME)
    response = dynamodb.Table(tableName).put_item(
        Item={
            VALUE_COLUMN: json.loads(json.dumps(stat), parse_float=Decimal),
            LOCATION_COLUMN: id,
            TIME_COLUMN: dateTime["ms"]
        }
    )
    wait(UPDATE_REQUEST_WAIT_TIME)
    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Complete.")
    else:
        raise Exception(f"There was an issue pushing value for {id} to table {tableName}.")


def lowerProvisionForTableOffset(offsetDays, now):
    targetDate = now + timedelta(offsetDays)
    targetDateTime = getDateTime(targetDate)
    targetTableName = getTableName(targetDateTime["date"])
    print(f"Attempting to lower provisions for table {targetTableName}...", end="    ")
    wait(READABLE_WAIT_TIME)
    if checkIfCurrentTableExists(targetTableName):
        table = dynamodb.Table(targetTableName).update(
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1
            }
        )
        wait(UPDATE_REQUEST_WAIT_TIME)
        if table:
            print("Complete.")
    else:
        print(f"Table {targetTableName} doesn't exist! No need to lower provisions.")




def getTableName(dateString):
    return f"{TABLE_PREFIX}{dateString}"


def setAdmin():
    print("This instance can create new tables with admin priviledge.")
    global ADMIN
    ADMIN = True
    wait(READABLE_WAIT_TIME)


def doOffset():
    if FLAG_OFFSET in sys.argv:
        offsetSeconds = randint(1, 5)
        print(f"Offsetting reading for {offsetSeconds} seconds...", end="    ")
        wait(offsetSeconds)
        print("Complete.")


def usage():
    print("")
    print(f"Usage: python3 {sys.argv[0]} [location]", end="\n\n")
    print("Optional flags:")
    print("    -a    [admin] Use to allow script to create and alter tables")
    print("    -o    [offset] Use to add a random offset up to 10 second to beginning of script")


def main(location):
    previousMinute = 0
    while(True):
        currentMinute = datetime.now().minute
        if (currentMinute != previousMinute):
            print("Start.")
            previousMinute = currentMinute
            if FLAG_OFFSET in sys.argv:
                doOffset()
            # get current time
            dateTime = getDateTime(datetime.now())
            tableName = getTableName(dateTime["date"])
            # tableName = testTableName
            print(f"The table to store next reading is {tableName}.")
            wait(READABLE_WAIT_TIME)
            # get current temperature from sensor
            temperature = getSensorReading()
            print (f'The temperature in {location} at {dateTime["time"]} is {temperature}{TEMPERATURE_UNIT}.')
            wait(READABLE_WAIT_TIME)
            # check if table for today exists
            if ADMIN:
                if not checkIfCurrentTableExists(tableName):
                    print(f"Table {tableName} DOES NOT exist!")
                    # create new table for today
                    createNewTable(tableName)
                    # lower provisioned throughput of older tables
                    lowerProvisionForTableOffset(-1, dateTime["source"])
            wait(READABLE_WAIT_TIME)
            # push stat to today's table
            if checkIfCurrentTableExists(tableName):
                # print("")
                pushStat(temperature, location, dateTime, tableName)
            else:
                print(f"Table {tableName} DOES NOT exist!")
            wait(READABLE_WAIT_TIME)
            finishedDateTime = getDateTime(datetime.now())
            # timeTaken = int((finishedDateTime["ms"] - dateTime["ms"]) / 1000)
            # sys.exit(f"Completed in {timeTaken}s.")
        else:
            print("Waiting...")
            wait(5)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit()
    if FLAG_ADMIN in sys.argv:
        setAdmin()
    # if FLAG_PERIOD in sys.argv:
        # setPeriod()
            
        
    main(sys.argv[1])
