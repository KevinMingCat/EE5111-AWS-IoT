from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from datetime import datetime
import csv
import os
import json

# A random programmatic shadow client ID.
SHADOW_CLIENT = "A0195042J_Raspi"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a1loa9sgjgf5z0-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = "4d2dec1d7b-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "4d2dec1d7b-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0195042J_Raspi"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
  CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
  SHADOW_HANDLER, True)


#object of CSV file
f = 'rainfall-monthly-total.csv'

#Initialize weather data with 2 keyword: 'daq_time' & 'rain'
WeatherData = {'daq_time':[], 'rain':[]}

#open rain fall csv file from directory (same directory where this script is stored)
with open(os.path.join(os.getcwd(),f)) as csvfile:
    readCSV = csv.reader(csvfile,delimiter=',')
    #skip header
    next(readCSV)

    for row in readCSV:
        WeatherData['daq_time'].append(row[0])
        WeatherData['rain'].append(row[1])

#length of data
Length = len(WeatherData['daq_time'])
#number of digit in Length
Num = len(str(abs(Length)))

#previous time
pt = datetime.now()
#hold time = 1s
delay = 1.0

Matric_Number = 'A0195042J'

for i in range(int(Length/2),Length):
    #current time
    ct = datetime.now()
    #timestamp
    timestamp = str(datetime.utcnow)
    id = str(i+1).zfill(Num)
    temp = {
        "state": {
            "reported":{
                "id": id,
                "timestamp": timestamp,
                "Matric_No": Matric_Number,
                "DaqDate": str(WeatherData['daq_time'][i]),
                "RainFall": str(WeatherData['rain'][i])
            }
        }
    }

    #create Json string
    msg = json.dumps(temp)

    #publish Json message to AWS IoT device shadow
    myDeviceShadow.shadowUpdate(msg,myShadowUpdateCallback,5)
    #wait for 5s

    while ((ct-pt).total_seconds() <= delay):
        #update current time
        ct = datetime.now()
    #update previous time
    pt = ct
