# firestore_db.py
import os
import json
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account
from datetime import datetime

def get_firestore_client(secret_name="firebase", env_name="FIREBASE_KEY_PATH"):
    """
    Devuelve cliente Firestore:
    - primero intenta st.secrets[secret_name] (para Streamlit Cloud)
    - si no existe, intenta la ruta local en env FIREBASE_KEY_PATH (para ejecución local)
    """
    import streamlit as st
    try:
        # intentar cargar desde streamlit secrets
        raw = st.secrets.get(secret_name)
        if raw:
            if isinstance(raw, dict):
                info = raw
            else:
                import json
                info = json.loads(raw)
            if "private_key" in info and "\\n" in info["private_key"]:
                info["private_key"] = info["private_key"].replace("\\n", "\n")
            import firebase_admin
            from firebase_admin import credentials, firestore
            if not firebase_admin._apps:
                cred = credentials.Certificate(info)
                firebase_admin.initialize_app(cred)
            return firestore.client()
    except Exception as ex:
        # si falla, seguimos al fallback local
        pass

    # fallback local desde ruta en variable de entorno
    path = os.getenv(env_name)
    if path and os.path.exists(path):
        import json
        info = json.load(open(path, "r", encoding="utf-8"))
        if "private_key" in info and "\\n" in info["private_key"]:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        import firebase_admin
        from firebase_admin import credentials, firestore
        if not firebase_admin._apps:
            cred = credentials.Certificate(info)
            firebase_admin.initialize_app(cred)
        return firestore.client()

    raise RuntimeError("No se pudo inicializar Firestore: añade st.secrets['%s'] o define env %s" % (secret_name, env_name))

# --- Inserts ---
def insert_command_firestore(client, cmd_start=0, cmd_stop=0, cmd_estop=0, sp_ref_cm=None):
    doc = {
        "ts": firestore.SERVER_TIMESTAMP,
        "cmd_start": int(bool(cmd_start)),
        "cmd_stop": int(bool(cmd_stop)),
        "cmd_estop": int(bool(cmd_estop)),
        "sp_ref_cm": None if sp_ref_cm is None else float(sp_ref_cm)
    }
    client.collection("control_commands").add(doc)

def insert_event_firestore(client, event_type, details):
    doc = {"ts": firestore.SERVER_TIMESTAMP, "event_type": event_type, "details": details}
    client.collection("event_log").add(doc)

def insert_telemetry_firestore(client, level_cm, vfd_rpm, vfd_speedcmd, blink_2hz, reached_sp, low_level, high_level):
    doc = {
        "ts": firestore.SERVER_TIMESTAMP,
        "level_cm": float(level_cm),
        "vfd_rpm": float(vfd_rpm),
        "vfd_speedcmd": float(vfd_speedcmd),
        "blink_2hz": int(bool(blink_2hz)),
        "reached_sp": int(bool(reached_sp)),
        "low_level": int(bool(low_level)),
        "high_level": int(bool(high_level))
    }
    client.collection("telemetry_samples").add(doc)

# --- Reads ---
def get_latest_telemetry(client, limit=200):
    q = client.collection("telemetry_samples").order_by("ts", direction=firestore.Query.DESCENDING).limit(limit)
    docs = q.stream()
    rows = []
    for d in docs:
        data = d.to_dict()
        ts = data.get("ts")
        # ts ya viene como datetime si está materializado; si no, puede llegar None momentáneamente
        rows.append({
            "ts": ts if ts is not None else datetime.utcnow(),
            "level_cm": data.get("level_cm", 0.0),
            "vfd_rpm": data.get("vfd_rpm", 0.0),
            "vfd_speedcmd": data.get("vfd_speedcmd", 0.0),
            "blink_2hz": data.get("blink_2hz", 0),
            "reached_sp": data.get("reached_sp", 0),
            "low_level": data.get("low_level", 0),
            "high_level": data.get("high_level", 0),
        })
    import pandas as pd
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values("ts")
    return df

def get_recent_events_firestore(client, limit=50):
    q = client.collection("event_log").order_by("ts", direction=firestore.Query.DESCENDING).limit(limit)
    docs = q.stream()
    rows = []
    for d in docs:
        data = d.to_dict()
        rows.append({"ts": data.get("ts"), "event_type": data.get("event_type"), "details": data.get("details")})
    import pandas as pd
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("ts", ascending=False)
    return df


