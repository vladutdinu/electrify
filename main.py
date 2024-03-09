import sesd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from typing import Optional
from datetime import datetime
import uvicorn

app = FastAPI()

# Load your dataset here
df = pd.read_csv('dataset.csv', parse_dates=['timestamp'])[["meter","timestamp","meter_reading_scaled"]]

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
    return filtered_df.to_dict(orient='records')

@app.get("/read/line")
async def line_chart_data():
    # Grouping by 'meter' to get readings for each room
    line_chart_data = df.groupby('meter')['meter_reading_scaled'].apply(list).to_dict()
    return line_chart_data

@app.get("/read/bar")
async def bar_chart_data():
    # Summing up readings by month
    monthly_sum = df.groupby(df['month_name'])['meter_reading_scaled'].sum().reset_index()
    bar_data = monthly_sum[['month_name', 'meter_reading_scaled']].to_dict(orient='records')
    data = {
        "consumption": monthly_sum['meter_reading_scaled'].to_list(),
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
            "type": "error",
            "message" : "Be aware of high energy consumption in Room 1",
            "content": ["There was a spike in the energy consumption in Room 1 in the past hour.", "Please verify the pluged in devices.", "If this is happening more frequently, please check the devices and find the ones which are causing the problems!", "Take in consideration that they might be old and need replaced."]
        },
        {
            "id": 2,
            "title" : "Devices that can be replaced",
            "type": "info",
            "message" : "Here are a list of devices that can be replaced in Room 1",
            "content": ["The TV, it appears it is an old model with the energy class G, you can find a new model with a better energy efficency", 
                        "The lightbulbs, it seems that you are still using old ones, consider replacing it with LED ones"]
        },
        {
            "id": 3,
            "title" : "Montly consumption over the average in Room 2",
            "message" : "Your energy consumption on Room 2 this month has passed the average of the last 6 months",
            "content": ["It looks like your monthly energy consumption on Room 2 passed the average of the last months.", "Please be aware of this event and take action!"]
        }
    ]
    return notifs

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)