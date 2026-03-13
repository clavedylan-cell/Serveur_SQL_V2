import pandas as pd
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from pydantic import BaseModel
from datetime import datetime, timezone 
import os

# DATABASE CORE
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./can_raw.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# MODÈLE UNIQUE (Optimisé pour IA)
class RawCan(Base):
    __tablename__ = "raw_can"
    id        = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True) # Index pour les séries temporelles
    can_id    = Column(String(10), index=True)
    valeur    = Column(String(50))

Base.metadata.create_all(bind=engine)

# --- API ---
app = FastAPI(title="CAN Data Ingestor")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class CanEntry(BaseModel):
    id: str
    val: str

@app.post("/ingest")
async def ingest_can(data: CanEntry, db: Session = Depends(get_db)):
    """Route ultra-rapide pour l'ESP32/iPhone"""
    new_frame = RawCan(can_id=data.id, valeur=data.val)
    db.add(new_frame)
    db.commit()
    return {"s": 1} # Réponse minimale pour gagner de la bande passante

@app.get("/export")
def get_all(db: Session = Depends(get_db)):
    """Récupère tout pour ton IA"""
    return db.query(RawCan).all()

@app.get("/data-for-ai")
def get_ai_table():
    db = SessionLocal()
    # 1. Charger les données brutes
    query = db.query(RawCan).all()
    db.close()
    
    if not query:
        return {"error": "no data"}

    # 2. Utiliser Pandas pour transformer le vrac en tableau propre
    df = pd.DataFrame([{
        'timestamp': r.timestamp, 
        'can_id': r.can_id, 
        'valeur': r.valeur
    } for r in query])

    # 3. Le Pivot : On transforme les lignes d'ID en colonnes
    # On regroupe par timestamp et on déploie les can_id en colonnes
    pivot_df = df.pivot(index='timestamp', columns='can_id', values='valeur')
    
    # 4. Nettoyage (optionnel : remplir les trous par la dernière valeur connue)
    pivot_df = pivot_df.ffill() 

    return pivot_df.reset_index().to_dict(orient="records")