from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel
from datetime import datetime, timezone
import os

# --- CONFIGURATION ---
DATABASE_URL = "sqlite:///./ebike_training.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

vitesse_pedale = 0x0D2
vitesse_roue = 0x0D1
SoC = 0x111
couple = 0x206
mode_actuel = 0x03B
puissance = 0x0D4

# --- MODÈLE SQL (Ton tableau d'entraînement) ---
class Telemetry(Base):
    __tablename__ = "telemetry"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    
    vitesse_pedale = Column(Float)
    vitesse_roue = Column(Float)
    Soc = Column(Float)
    couple = Column(Float)
    mode_actuel = Column(Float)
    puissance = Column(Float)

Base.metadata.create_all(bind=engine)
app = FastAPI()

# --- SCHÉMA DE RÉCEPTION ---
class TelemetryIn(BaseModel):
    vitesse_pedale: float
    vitesse_roue: float
    Soc: float
    couple: float
    mode_actuel: float
    puissance: float

# --- ROUTE D'ENREGISTREMENT ---
@app.post("/log")
async def log_data(data: TelemetryIn):
    db = SessionLocal()
    try:
        new_row = Telemetry(
            vitesse_pedale=data.vitesse_pedale,
            vitesse_roue=data.vitesse_roue,
            Soc=data.Soc,
            couple=data.couple,
            mode_actuel=data.mode_actuel,
            puissance=data.puissance
        )
        db.add(new_row)
        db.commit()
    finally:
        db.close()
    return {"status": "saved"}

# --- ROUTE POUR RÉCUPÉRER TOUT LE TABLEAU (IA) ---
@app.get("/export")
def export_data():
    db = SessionLocal()
    data = db.query(Telemetry).all()
    db.close()
    return data