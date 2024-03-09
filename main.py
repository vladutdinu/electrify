import sesd
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from typing import Optional
from datetime import datetime
import uvicorn

app = FastAPI()

# Load your dataset here
df = pd.read_csv('dataset.csv')

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

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000, reload=True)