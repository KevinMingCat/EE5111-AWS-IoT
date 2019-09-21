from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from gpiozero import CPUTemperature
from datetime import datetime
from time import sleep, strftime, time
import os
import csv
import json


# A random programmatic shadow client ID.
SHADOW_CLIENT = "A0195042J_Raspi_Monitor"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a1loa9sgjgf5z0-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = "a9589dcfe7-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "a9589dcfe7-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0195042J_Raspi_Monitor"

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


# create object - cpu
cpu = CPUTemperature()

# initialize count
count = 0
# matric number
Matric_Number = 'A0195042J'

#create the log csv file
#with open('/deviceSDK/aws-iot-device-sdk-python/Raspi_Temp.csv','a') as log:
with open('Raspi_Temp.csv','a') as log:
  while count < 20:
    count = count + 1
      
    temp = cpu.temperature
      
    now = datetime.now()
    date_time = now.strftime('%Y-%m-%d %H:%M:%S')
    log.write('{0},{1}\n'.format(date_time,str(temp)))
    sleep(1)
      
    msg = {
      "state": {
          "reported": {
              "id": str(count),
              "timestamp": date_time,
              "Matric_No": Matric_Number,
              "temp": str(temp)
          }
      }
    }
    myDeviceShadow.shadowUpdate(json.dumps(msg),myShadowUpdateCallback,5)
    # rest for 5s before next data collection
    sleep(5)
  
