import sesd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from typing import Optional
from datetime import datetime
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load your dataset here
df = pd.read_csv('dataset.csv', parse_dates=['timestamp'])[["meter","timestamp","meter_reading_scaled"]]
df['day'] = df['timestamp'].dt.date
df['month'] = df['timestamp'].dt.month
df['quarter'] = df['timestamp'].dt.quarter
df['month_name'] = df['timestamp'].dt.month_name()



@app.get("/readings/{meter_id}")
async def get_readings(meter_id: int, start: Optional[str] = None, end: Optional[str] = None):
    # Filter the DataFrame for the specified meter ID
    filtered_df = df[df['meter'] == meter_id]
    
    # If start and end params are provided, further filter the DataFrame
    if start and end:
        start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        filtered_df = filtered_df[(filtered_df['timestamp'] >= start) & (filtered_df['timestamp'] <= end)]
    
    # Check if the filtered DataFrame is empty
    if filtered_df.empty:
        raise HTTPException(status_code=404, detail=f"No readings found for meter {meter_id} with the provided timeframe.")

    # Convert the filtered DataFrame to a dictionary and return it
    return filtered_df.groupby(filtered_df['timestamp'].dt.to_period("d")).to_dict(orient='records')

@app.get("/read/line")
async def line_chart_data():
    # Grouping by 'meter' to get readings for each room
    df = df.groupby(['meter', 'day'])['meter_reading_scaled'].sum().reset_index()
    outliers_indices_1 = sesd.seasonal_esd(df[df["meter"]==1]["meter_reading_scaled"].values, periodicity = 30,  max_anomalies=len(df[df["meter"]==1]["meter_reading_scaled"].values)//8, alpha = 3)
    outliers_indices_3 = sesd.seasonal_esd(df[df["meter"]==3]["meter_reading_scaled"].values, periodicity = 30,  max_anomalies=len(df[df["meter"]==3]["meter_reading_scaled"].values)//8, alpha = 3)

    marks_1=[]
    data = df[df["meter"]==1]["meter_reading_scaled"].values
    for i in range(len(data)):
        if i in outliers_indices_1:
            marks_1.append(data[i])

    marks_3=[]
    data = df[df["meter"]==3]["meter_reading_scaled"].values
    for i in range(len(data)):
        if i in outliers_indices_3:
            marks_3.append(data[i])

    th = [np.average(marks_1), np.average(marks_3)]
    data = [{
        "id": 1,
            "consumption": df[df['meter'] == 1],
            "threshold": [th[0] for x in range(len(df[df['meter'] == 1))]
        },
        {
            "id": 3,
            "consumption": df[df['meter'] == 3],
            "threshold":  [th[1] for x in range(len(df[df['meter'] == 3]))]
        }
    ]
    return data

@app.get("/read/bar")
async def bar_chart_data():
    # Summing up readings by month
    monthly_sum = df.groupby(df['month_name'])['meter_reading_scaled'].sum().reset_index()
    bar_data = monthly_sum[['month_name', 'meter_reading_scaled']].to_dict(orient='records')
    data = {
        "consumption": monthly_sum['meter_reading_scaled'].to_list()/100,
        "months": monthly_sum['month_name'].to_list()
    }
    return data

@app.get("/read/pie")
async def pie_chart_data():
    # Summing up readings by quarter
    quarter_sum = df.groupby('quarter')['meter_reading_scaled'].sum()
    total = quarter_sum.sum()
    # Calculating the percentage of each quarter
    quarter_percentage = (quarter_sum / total * 100).round(2)
    pie_data = {
        "quarters": ["Q"+str(i) for i in quarter_sum.index.tolist()],
        "quartersConsumption": quarter_sum.values.tolist(),
        "quartersPercentages": quarter_percentage.values.tolist()
    }
    return pie_data

@app.get("/read/notification")
async def notif():
    # Summing up readings by quarter
    notifs = [
        {
            "id": 1,
            "title" : "High consumption detected!",
            "type": "ERROR",
            "message" : "Be aware of high energy consumption in Room 1",
            "content": ["There was a spike in the energy consumption in Room 1 in the past hour.", "Please verify the pluged in devices.", "If this is happening more frequently, please check the devices and find the ones which are causing the problems!", "Take in consideration that they might be old and need replaced."]
        },
        {
            "id": 2,
            "title" : "Devices that can be replaced",
            "type": "INFO",
            "message" : "Here are a list of devices that can be replaced in Room 1",
            "content": ["The TV, it appears it is an old model with the energy class G, you can find a new model with a better energy efficency", 
                        "The lightbulbs, it seems that you are still using old ones, consider replacing it with LED ones"]
        },
        {
            "id": 3,
            "title" : "Montly consumption over the average in Room 2",
            "type": "WARNING",
            "message" : "Your energy consumption on Room 2 this month has passed the average of the last 6 months",
            "content": ["It looks like your monthly energy consumption on Room 2 passed the average of the last months.", "Please be aware of this event and take action!"]
        }
    ]
    return notifs

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)