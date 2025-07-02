import Metashape
from pprint import pprint
client = Metashape.NetworkClient()
client.connect('192.168.1.179')

list  = client.batchList()

print("Batches:", len(list))

for batch in list:
   pprint(batch)


id = client.findBatch("CAN/ABC/LAF/SPLW/2409/3dprocessing/metashape/CAN_ABC_LAF_SPLW_2409_3dprocessing.psx")

print(f"Batch ID: {id}")
for i in range(100):
   try:
      client.abortBatch(i)
      print(f"Batch {i} aborted successfully.")
   except Exception as e:
      print(f"Failed to abort batch {i}: {str(e)}")

# info = client.batchInfo(14)

# pprint(info)

# client.setBatchPaused(id, False)
# print(f"Batch {id} unpaused.")

# import time
# time.sleep(5)

# info = client.batchInfo(id)

# print(f"Updated Batch Info: {info}")