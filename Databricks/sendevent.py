import random
import datetime
import json
from azure.eventhub import EventHubProducerClient, EventData

# ✅ Replace with your actual Event Hub connection string and name
EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = ""

# Constants
machine_ids = ["NTX-1", "NTX-2", "NTX-3", "NTX-4", "NTX-5"]
signals = ["operate", "alarm", "on", "off", "suspend", "disconnect"]

# Initialize Event Hub producer
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

# Number of records to send
num_records = 100

# Create initial batch
event_data_batch = producer.create_batch()

for i in range(num_records):
    machine_id = random.choice(machine_ids)
    signal = random.choice(signals)

    # Create flat JSON record
    record = {
        "L1": machine_id,
        "signal": signal,
        "value": random.randint(20, 70),
        "updateDateUTC": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "timespan": f"{random.randint(4, 40)}ms"
    }

    # Convert to JSON
    json_data = json.dumps(record)
    print(f" Prepared record {i+1}: {json_data}")  

    event = EventData(json_data)

    try:
        event_data_batch.add(event)
    except ValueError:
        # If current batch is full, send and create new one
        producer.send_batch(event_data_batch)
        print(f" Sent full batch before record {i+1}")
        event_data_batch = producer.create_batch()
        event_data_batch.add(event)

# Send remaining records
if len(event_data_batch) > 0:
    producer.send_batch(event_data_batch)
    print(" Sent final batch")

# Close producer
producer.close()
