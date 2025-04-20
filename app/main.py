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
    timestamp = Column(DateTime, default=datetime.utcnow)
    co2 = Column(Float)
    so2 = Column(Float)
    hf = Column(Float)

Base.metadata.create_all(bind=engine)

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
        "location": "Transformation Reactor",
        "co2": 0.10,
        "so2": 0.001,
        "hf": 0.00005
    },
    "checkpoint_4": {
        "location": "Final Emission",
        "co2": 0.08,
        "so2": 0.00007,
        "hf": 0.000005
    }
}

def generate_gas_data(means, std_frac=0.05):
    timestamp = datetime.utcnow()
    data_list = []
    for i, (checkpoint, data) in enumerate(means.items(), start=1):
        noisy = {}
        for gas, mu in data.items():
            if gas != "location":
                sigma = abs(mu) * std_frac
                sample = max(np.random.normal(mu, sigma), 0.0)
                noisy[gas] = sample
        checkpoint_data = {
            "sensor_id": i,
            "checkpoint_name": data["location"],
            "timestamp": timestamp,
            **noisy
        }
        data_list.append(checkpoint_data)
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
                    timestamp=entry["timestamp"],
                    co2=entry["co2"],
                    so2=entry["so2"],
                    hf=entry["hf"]
                )
                session.add(gas_entry)
                entry["timestamp"] = entry["timestamp"].isoformat()
                await websocket.send_json(entry)

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
    uvicorn.run("app.main:app", host="0.0.0.0", port=8086, reload=True)