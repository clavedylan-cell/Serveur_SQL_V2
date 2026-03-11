from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
import os
import time
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, String, text
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timezone

# ─── DATABASE ───────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./ebike.db")

# Render fournit "postgres://" mais SQLAlchemy veut "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = sqlalchemy.orm.declarative_base()

# ─── MODÈLES SQL ────────────────────────────────────────────
class TripData(Base):
    """Table principale : chaque ligne = un segment de trajet."""
    __tablename__ = "trip_data"
    id            = Column(Integer, primary_key=True, index=True)
    timestamp     = Column(DateTime, default=datetime.utcnow, index=True)
    distance      = Column(Float)
    voltage       = Column(Float)
    current       = Column(Float)
    puissance_instantannée = Column(Float)
    speed         = Column(Float)
    torque        = Column(Float)
    assist_level  = Column(Integer)

class RawCan(Base):
    """Table secondaire : chaque trame CAN brute reçue."""
    __tablename__ = "raw_can"
    id        = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    can_id    = Column(String(20), index=True)
    valeur    = Column(String(100))

Base.metadata.create_all(bind=engine)

# ─── APP ────────────────────────────────────────────────────
app = FastAPI(title="eBike Telemetry", version="2.0")

# Dependency injection : session DB par requête
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ─── SCHÉMAS PYDANTIC ───────────────────────────────────────
class TelemetrySegment(BaseModel):
    dist: float
    v:    float   # Voltage
    a:    float   # Ampères
    kmh:  float   # Vitesse
    nm:   float   # Couple
    w:    float
    mode: int     # Niveau d'assistance

class CanData(BaseModel):
    can_id:    str
    valeur:    str
    timestamp: Optional[float] = None

# ─── ROUTES ─────────────────────────────────────────────────
@app.get("/")
def read_root():
    return {"status": "online", "message": "eBike Telemetry v2"}

@app.post("/log-segment")
def log_segment(data: TelemetrySegment, db: Session = Depends(get_db)):
    """Reçoit un segment complet (distance + toutes les métriques)."""
    entry = TripData(
        distance     = data.dist,
        voltage      = data.v,
        current      = data.a,
        speed        = data.kmh,
        torque       = data.nm,
        puissance_instantannée = data.w,
        assist_level = data.mode
    )
    db.add(entry)
    db.commit()
    return {"status": "saved", "segment": data.dist}

@app.post("/debug-can")
def receive_can(data: CanData, db: Session = Depends(get_db)):
    """Reçoit une trame CAN brute et la stocke."""
    ts = datetime.fromtimestamp(data.timestamp, tz=timezone.utc) if data.timestamp else datetime.now(timezone.utc)
    entry = RawCan(timestamp=ts, can_id=data.can_id, valeur=data.valeur)
    db.add(entry)
    db.commit()
    print(f"CAN | ID: {data.can_id} | Valeur: {data.valeur} | {ts}")
    return {"status": "saved", "can_id": data.can_id}

# ─── LECTURE TEMPS RÉEL ─────────────────────────────────────
@app.get("/live")
def live_data(db: Session = Depends(get_db)):
    """Dernier segment enregistré (dashboard temps réel)."""
    last = db.query(TripData).order_by(TripData.timestamp.desc()).first()
    if not last:
        return {"status": "empty"}
    return {
        "timestamp":    last.timestamp,
        "distance":     last.distance,
        "voltage":      last.voltage,
        "current":      last.current,
        "speed":        last.speed,
        "torque":       last.torque,
        "puissance_instantannée": last.puissance_instantannée,
        "assist_level": last.assist_level
    }

@app.get("/live-can/{can_id}")
def live_can(can_id: str, db: Session = Depends(get_db)):
    """Dernière valeur reçue pour un ID CAN spécifique."""
    last = (db.query(RawCan)
              .filter(RawCan.can_id == can_id)
              .order_by(RawCan.timestamp.desc())
              .first())
    if not last:
        raise HTTPException(status_code=404, detail=f"ID {can_id} inconnu")
    return {"can_id": can_id, "valeur": last.valeur, "timestamp": last.timestamp}

# ─── EXPORT ─────────────────────────────────────────────────
@app.get("/history")
def get_history(limit: int = 100, db: Session = Depends(get_db)):
    """Historique des N derniers segments."""
    rows = (db.query(TripData)
              .order_by(TripData.timestamp.desc())
              .limit(limit).all())
    return [
        {
            "timestamp":    r.timestamp,
            "distance":     r.distance,
            "voltage":      r.voltage,
            "current":      r.current,
            "speed":        r.speed,
            "torque":       r.torque,
            "puissance_instantannée": r.puissance_instantannée,
            "assist_level": r.assist_level
        } for r in rows
    ]

@app.get("/export-csv")
def export_csv(db: Session = Depends(get_db)):
    """Export CSV pivot : une colonne par ID CAN."""
    rows = db.query(RawCan).order_by(RawCan.timestamp).all()
    if not rows:
        return {"status": "empty"}

    # Pivot en mémoire : { timestamp -> { can_id -> valeur } }
    from collections import defaultdict
    pivot = defaultdict(dict)
    all_ids = set()
    for r in rows:
        key = r.timestamp.isoformat()
        pivot[key][r.can_id] = r.valeur
        all_ids.add(r.can_id)

    import csv, io
    output = io.StringIO()
    cols = ["timestamp"] + sorted(all_ids)
    writer = csv.DictWriter(output, fieldnames=cols)
    writer.writeheader()
    for ts, values in sorted(pivot.items()):
        writer.writerow({"timestamp": ts, **values})

    return {"status": "ok", "csv": output.getvalue(), "rows": len(pivot)}

@app.delete("/reset")
def reset(db: Session = Depends(get_db)):
    """Vide toutes les tables."""
    db.query(TripData).delete()
    db.query(RawCan).delete()
    db.commit()
    return {"status": "reset"}
