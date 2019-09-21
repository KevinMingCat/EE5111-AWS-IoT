from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time, datetime

# The unique hostname (can be found in setting) AWS IoT generates for this device.
HOST_NAME = "a1loa9sgjgf5z0-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for AWS IoT, 
ROOT_CA = "AmazonRootCA1.pem"

# A random programmatic shadow client ID for both Things (engine1 and engine3)
SHADOW_CLIENT_1 = "A0195042J_EngineFD001"
SHADOW_CLIENT_2 = "A0195042J_EngineFD003"


# The relative path to your private key file that AWS IoT generates for this device
PRIVATE_KEY_1 = "ecf87423da-private.pem.key"
PRIVATE_KEY_2 = "ef8e4b716b-private.pem.key"

# The relative path to your certificate file that AWS IoT generates for this device
CERT_FILE_1 = "ecf87423da-certificate.pem.crt"
CERT_FILE_2 = "ef8e4b716b-certificate.pem.crt"

# A programmatic shadow handler name prefix for both Things (engine1 and engine3)
SHADOW_HANDLER_1 = "A0195042J_EngineFD001"
SHADOW_HANDLER_2 = "A0195042J_EngineFD003"

# *****************************************************
# Main script runs from here onwards.
# To stop running this script, press Ctrl+C.
# *****************************************************

with open('train_FD001.txt','r') as infile_1:
  with open('FD001_out.txt','a') as outfile_1:
    # Due to process time issue, only read the 1st 5000 lines of data
    for line in infile_1.readlines()[0:5000]:
        outfile_1.write(line)

with open('train_FD003.txt','r') as infile_2:
  with open('FD003_out.txt','a') as outfile_2:
    # Due to process time issue, only read the 1st 5000 lines of data
    for line in infile_2.readlines()[0:5000]:
        outfile_2.write(line)

# Get the Data labels, later will be used as header of table inside DynamoDB 
sensor_name = ['s'+ str(i) for i in range(1,22)]
dataLabels = ['id', 'timestamp', 'Matric_Number', 'cycle', 'os1', 'os2', 'os3'] + sensor_name

Matric_Number = 'A0195042J'

for i in range(0,len(dataLabels)):
    dataLabels[i] = '\"' + dataLabels[i] + '\"'

process_1 = open("FD001_out.txt",'r')
process_2 = open("FD003_out.txt",'r')

dataString_1 = []
dataString_2 = []
modifiedData_1 = []
modifiedData_2 = []

head = '{"state":{"reported":{'
tail = '}}}'

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback_1(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER_1 + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient_1 = AWSIoTMQTTShadowClient(SHADOW_CLIENT_1)
myShadowClient_1.configureEndpoint(HOST_NAME, 8883)
myShadowClient_1.configureCredentials(ROOT_CA, PRIVATE_KEY_1,
  CERT_FILE_1)
myShadowClient_1.configureConnectDisconnectTimeout(10)
myShadowClient_1.configureMQTTOperationTimeout(5)
myShadowClient_1.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow_1 = myShadowClient_1.createShadowHandlerWithName(
  SHADOW_HANDLER_1, True)

for x in process_1.readlines():
      newData_1 = x.split(" ")
      modifiedData_1 = []
      modifiedData_1.append(str('FD001_' + newData_1[0]))
      modifiedData_1.append(str(datetime.datetime.utcnow()))
      modifiedData_1.append(Matric_Number)
      for j in range(2,len(sensor_name)):
        modifiedData_1.append(newData_1[j])      
    
      ColumnLabels = []
      ColumnLabels.append(str(dataLabels[0] + ':'))
      ColumnLabels.append(str('"' + modifiedData_1[0] + '",'))
      ColumnLabels.append(str(dataLabels[1] + ':'))
      ColumnLabels.append(str('"' + str(datetime.datetime.now()) + '",'))
      ColumnLabels.append(str(dataLabels[2] + ':'))
      ColumnLabels.append(str('"' + Matric_Number + '",'))
    
   
      for i in range(3,len(dataLabels)):
        ColumnLabels.append(str(dataLabels[i] + ':'))
        ColumnLabels.append(str('"' + newData_1[i-2] + '",'))
    
      string = ''.join(ColumnLabels)
      string = string[:-1]
    
      data = []
      data.append(head)
      data.append(string)
      data.append(tail)
      data.append('\n')
      dataString_1 = ''.join(data)
      print(dataString_1)
    
      myDeviceShadow_1.shadowUpdate(dataString_1,myShadowUpdateCallback_1, 5)
      # read the data each 1s
      time.sleep(1)

# duplication for the second thing (engineFD003)
# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback_2(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER_2 + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient_2 = AWSIoTMQTTShadowClient(SHADOW_CLIENT_2)
myShadowClient_2.configureEndpoint(HOST_NAME, 8883)
myShadowClient_2.configureCredentials(ROOT_CA, PRIVATE_KEY_2,
  CERT_FILE_2)
myShadowClient_2.configureConnectDisconnectTimeout(10)
myShadowClient_2.configureMQTTOperationTimeout(5)
myShadowClient_2.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow_2 = myShadowClient_2.createShadowHandlerWithName(
  SHADOW_HANDLER_2, True)

for y in process_2.readlines():
      newData_2 = y.split(" ")
      modifiedData_2 = []
      modifiedData_2.append(str('FD003_' + newData_2[0]))
      modifiedData_2.append(str(datetime.datetime.utcnow()))
      modifiedData_2.append(Matric_Number)
      for k in range(2,len(sensor_name)):
        modifiedData_2.append(newData_2[k])      

      ColumnLabels = []
      ColumnLabels.append(str(dataLabels[0] + ':'))
      ColumnLabels.append(str('"' + modifiedData_2[0] + '",'))
      ColumnLabels.append(str(dataLabels[1] + ':'))
      ColumnLabels.append(str('"' + str(datetime.datetime.now()) + '",'))
      ColumnLabels.append(str(dataLabels[2] + ':'))
      ColumnLabels.append(str('"' + Matric_Number + '",'))


      for l in range(3,len(dataLabels)):
        ColumnLabels.append(str(dataLabels[l] + ':'))
        ColumnLabels.append(str('"' + newData_2[l-2] + '",'))
      
      string = ''.join(ColumnLabels)
      string = string[:-1]
  
      data = []
      data.append(head)
      data.append(string)
      data.append(tail)
      data.append('\n')
      dataString_2 = ''.join(data)
      print(dataString_2)

      myDeviceShadow_2.shadowUpdate(dataString_2,myShadowUpdateCallback_2, 5)
      # read the data each 1s
      time.sleep(1)