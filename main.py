import sesd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from typing import Optional
from datetime import datetime
import uvicorn

app = FastAPI()

# Load your dataset here
df = pd.read_csv('dataset.csv')[["meter","timestamp","meter_reading_scaled"]]

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

@app.get("/readings/line")
async def line_chart_data():
    # Grouping by 'meter' to get readings for each room
    line_chart_data = df.groupby('meter')['meter_reading_scaled'].apply(list).to_dict()
    return line_chart_data

@app.get("/readings/bar")
async def bar_chart_data():
    # Summing up readings by month
    monthly_sum = df.groupby(df['timestamp'].dt.to_period("M"))['meter_reading_scaled'].sum().reset_index()
    monthly_sum['month'] = monthly_sum['timestamp'].dt.strftime('%Y-%m')
    bar_data = monthly_sum[['month', 'meter_reading_scaled']].to_dict(orient='records')
    return bar_data

@app.get("/readings/pie")
async def pie_chart_data():
    # Summing up readings by quarter
    quarter_sum = df.groupby('quarter')['meter_reading_scaled'].sum()
    total = quarter_sum.sum()
    # Calculating the percentage of each quarter
    quarter_percentage = (quarter_sum / total * 100).round(2)
    pie_data = {
        "quarters": quarter_sum.index.tolist(),
        "consumption": quarter_sum.values.tolist(),
        "percentages": quarter_percentage.values.tolist()
    }
    return pie_data

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)