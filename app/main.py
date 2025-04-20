import asyncio
import json
import time
import numpy as np
from fastapi import FastAPI, WebSocket
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

app = FastAPI()

DATABASE_URL = "sqlite:///./gas.db"
Base = declarative_base()
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class GasData(Base):
    __tablename__ = "gas_data"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, index=True)
    checkpoint_name = Column(String, index=True)
    gas_type = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    value = Column(Float)

Base.metadata.create_all(bind=engine)

# Define mean values for each checkpoint per gas
means = {
    "checkpoint_1": {
        "location": "Gas Capture Mechanism Input",
        "co2": 0.12,
        "so2": 0.005,
        "hf": 0.0002
    },
    "checkpoint_2": {
        "location": "Scrubber Input",
        "co2": 0.11,
        "so2": 0.0035,
        "hf": 0.00015
    },
    "checkpoint_3": {
        "location": "Final Emission",
        "co2": 0.08,
        "so2": 0.00007,
        "hf": 0.000005
    }
}

def generate_gas_data(means, std_frac=0.05):
    timestamp = datetime.utcnow()
    data_list = []
    sensor_id_counter = 1
    for checkpoint, data in means.items():
        location = data["location"]
        for gas in ["co2", "so2", "hf"]:
            mu = data[gas]
            sigma = abs(mu) * std_frac
            sample = max(np.random.normal(mu, sigma), 0.0)
            data_list.append({
                "sensor_id": sensor_id_counter,
                "checkpoint_name": location,
                "gas_type": gas,
                "timestamp": timestamp,
                "value": sample
            })
            sensor_id_counter += 1
    return data_list

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = generate_gas_data(means, std_frac=0.1)
        with SessionLocal() as session:
            three_hours_ago = datetime.utcnow() - timedelta(hours=3)
            session.query(GasData).filter(GasData.timestamp < three_hours_ago).delete()
            session.commit()

            for entry in data:
                gas_entry = GasData(
                    sensor_id=entry["sensor_id"],
                    checkpoint_name=entry["checkpoint_name"],
                    gas_type=entry["gas_type"],
                    timestamp=entry["timestamp"],
                    value=entry["value"]
                )
                session.add(gas_entry)
                # Send as JSON to frontend
                message = {
                    "sensor_id": entry["sensor_id"],
                    "checkpoint": entry["checkpoint_name"],
                    "gas": entry["gas_type"],
                    "value": entry["value"],
                    "timestamp": entry["timestamp"].isoformat()
                }
                await websocket.send_json(message)

            session.commit()

        await asyncio.sleep(3)

@app.get("/last-minute")
def get_last_minute_data():
    with SessionLocal() as session:
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
        data = session.query(GasData).filter(GasData.timestamp >= one_minute_ago).all()
        return [d.__dict__ for d in data]

@app.get("/last-30-minutes")
def get_last_30_minutes_data():
    with SessionLocal() as session:
        thirty_minutes_ago = datetime.utcnow() - timedelta(minutes=30)
        data = session.query(GasData).filter(GasData.timestamp >= thirty_minutes_ago).all()
        return [d.__dict__ for d in data]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
