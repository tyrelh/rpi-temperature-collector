import time
import datetime
import boto3
# from w1thermsensor import W1ThermSensor

dynamodb = boto3.client('dynamodb') # TODO: add region name
# sensor1 = W1ThermSensor


def getDate(now):
    return now.strftime("%Y-%m-%d")


def getTime(now):
    return now.strftime("%H:%M:%S")


def getDateTime(now):
    return {
        date: getDate(now),
        time: getTime(now)
    }


def checkIfCurrentTableExists(tableName):
    response = dynamodb.list_tables(
        ExclusiveStartTableName = tableName,
        Limit = 2
    )
    if "TableNames" in response:
        if len(response["TableNames"]) == 1 tableName in response["TableNames"]:
            if tableName in response["TableNames"]:
                return True
            elif
                return False
    raise Exception("There was an issue fetching dynamo tables", response)


def createNewTable(tableName):
    # creating a new table takes time
    # aws will return a CREATING status
    # table will switch to ACTIVE when it's ready
    # can use DescribeTable to check the status of a table
    response = dynamodb.create_table(
            AttributeDefinitions = [
                    {
                        "AttributeName": "Time",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": 
                    }
                ]
            )



def main():
    # get current time
    now = datetime.now()
    dateTime = getDateTime(now)
    # get current temperature from sensor
    temperature = sensor.get_temperature()
    print("The temperature at %s for sensor1 is %s c" % dateTime.time, temperature)

    # check if table for today exists
    if not checkIfCurrentTableExists(dateTime.date):
        # create new table for today
        createNewTable(dateTime.date)

    # push stat to table


    

if __name__ == '__main__':
    main()
