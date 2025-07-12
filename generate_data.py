import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="Your-GCP-Account-Key.json"
import random
import pandas as pd
from datetime import datetime,timedelta
from google.cloud import bigquery
NUM_RECORDS=25
PROJECT_ID="GCP_PROJECT_ID"
DATASET_ID="Your-dataset-ID"
TABLE_ID="your-table-ID"
MODES=["Local","Metro","Bus"]
LINES={
    "Local":["Western","Central","Harbour"],
    "Metro":["Line 2A","Line 1","Line 7"],
    "Bus":["302","66","521 LTD"]
}
STOPS=["Dadar","Churchgate","Wadala","Andheri","Borivali","Ghatkopar","DN Nagar","Worli","Thane"]
WEATHER_TYPES=["Sunny","Rainy","Foggy","Clear"]
def random_time():
    hour=random.randint(5,22)
    minute=random.choice([0,5,10,15,20,25])
    return datetime(2025,7,6,hour,minute)
def weather_delay(weather):
    if weather=="Rainy":
        return random.randint(5,15)
    elif weather=="Foggy":
        return random.randint(3,10)
    else:
        return random.randint(-2,5)
def inject_error():
    chance=random.random()
    if chance<0.05:
        return "Cancelled",None,None
    elif chance<0.10:
        return "Early",-random.randint(1,10),"Early"
    elif chance<0.15:
        return "On Time",0,"On Time"
    else:
        return "Normal",None,None
def transport_data():
    data=[]
    for _ in range(NUM_RECORDS):
        Mode=random.choice(MODES)
        Line=random.choice(LINES[Mode])
        Source,destination=random.sample(STOPS, 2)
        Scheduled_time=random_time()
        weather=random.choice(WEATHER_TYPES)
        status,injected_delay,label=inject_error()
        if status=="Cancelled":
            actual_arrival=None
            delay=None
            arrival_time_str = None
        elif injected_delay is not None:
            delay = injected_delay
            actual_arrival = Scheduled_time + timedelta(minutes=delay)
            arrival_time_str = actual_arrival.strftime("%H:%M:%S")
        else:
            delay = weather_delay(weather)
            actual_arrival = Scheduled_time + timedelta(minutes=delay)
            label = "Delayed" if delay > 0 else "On Time"
            arrival_time_str = actual_arrival.strftime("%H:%M:%S")

        data.append({
            "Mode": Mode,
            "Line": Line,
            "Source": Source,
            "Destination": destination,
            "Delay_Mins": delay,
            "Platform_Stop": random.randint(1, 10),
            "Weather": weather,
            "Status": label,
            "Scheduled_Time": Scheduled_time,
            "Actual_Arrival_Time": actual_arrival if actual_arrival else pd.NaT

        })
    return pd.DataFrame(data)
def bigq(df):
    df_simulated["Scheduled_Time"] = pd.to_datetime(df_simulated["Scheduled_Time"])
    df_simulated["Actual_Arrival_Time"] = pd.to_datetime(df_simulated["Actual_Arrival_Time"])
    client=bigquery.Client(project=PROJECT_ID)
    table_ref=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"{len(df)} rows uploaded to BigQuery table: {table_ref}")
    table = client.get_table(table_ref)
    print(f"Total rows in BigQuery table now: {table.num_rows}")
df_simulated=transport_data()
print(df_simulated.head())
bigq(df_simulated)
