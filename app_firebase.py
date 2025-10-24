import os
import pandas as pd
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json
# Nombre del secreto que guardaste en Streamlit (ajusta si usaste otro)
SECRET_KEY_NAME = "firebase_key"   # <- si tu secreto se llama firebase_key, pon ese nombre

# Verifica que exista el secreto
raw = st.secrets.get(SECRET_KEY_NAME)
if not raw:
    st.error(f"Error al inicializar Firebase: st.secrets has no key '{SECRET_KEY_NAME}'. Añádelo en Manage app → Settings → Secrets.")
    st.stop()

# raw puede ser 1) un dict ya parseado, 2) un string JSON (multilínea)
if isinstance(raw, dict):
    info = raw
else:
    # si es string, intentamos convertirlo a dict
    try:
        info = json.loads(raw)
    except Exception as e:
        st.error("Error al parsear el secreto de Firebase. Asegúrate de pegar el JSON EXACTO en Secrets (no variables extra).")
        st.write("Detalle técnico del parse error:", str(e))
        st.stop()

# Reparar private_key con saltos de línea si viene escapada (caso común)
pk = info.get("private_key")
if pk and "\\n" in pk:
    info["private_key"] = pk.replace("\\n", "\n")

# Comprobaciones básicas
if "project_id" not in info or "private_key" not in info or "client_email" not in info:
    st.error("El JSON de Firebase parece incompleto. Asegúrate de pegar el JSON completo de la cuenta de servicio.")
    st.stop()

# Inicializar Firebase (si no está inicializado aún)
try:
    # Evitar re-inicializar si ya existe app
    if not firebase_admin._apps:
        cred = credentials.Certificate(info)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    # opcional: mostrar un pequeño aviso (comentarlo en producción)
    st.write("✅ Firebase inicializado correctamente.")
except Exception as e:
    st.error("Error al inicializar Firebase (ver logs).")
    st.write("Detalle:", str(e))
    st.stop()
# ----------------- Configuración Firebase -----------------
# Inicializar Firebase solo una vez
if not firebase_admin._apps:
    # Opción 1: Usando archivo JSON de credenciales
    # cred = credentials.Certificate("firebase-credentials.json")
    
    # Opción 2: Usando variables de entorno (recomendado para producción)
    try:
        firebase_config = {
            "type": os.getenv("FIREBASE_TYPE") or st.secrets["firebase"]["type"],
            "project_id": os.getenv("FIREBASE_PROJECT_ID") or st.secrets["firebase"]["project_id"],
            "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID") or st.secrets["firebase"]["private_key_id"],
            "private_key": (os.getenv("FIREBASE_PRIVATE_KEY") or st.secrets["firebase"]["private_key"]).replace('\\n', '\n'),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL") or st.secrets["firebase"]["client_email"],
            "client_id": os.getenv("FIREBASE_CLIENT_ID") or st.secrets["firebase"]["client_id"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("FIREBASE_CERT_URL") or st.secrets["firebase"]["client_x509_cert_url"]
        }
        cred = credentials.Certificate("serviceAccountKey.json")
        
        # URL de tu Realtime Database
        database_url = os.getenv("FIREBASE_DATABASE_URL") or st.secrets["firebase"]["database_url"]
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': database_url
        })
    except Exception as e:
        st.error(f"Error al inicializar Firebase: {e}")
        st.stop()

# ----------------- Funciones de acceso a Firebase -----------------
def insert_command(cmd_start=0, cmd_stop=0, cmd_estop=0, sp_ref_cm=None):
    """Inserta comando de control en Firebase"""
    try:
        ref = db.reference('control_commands')
        timestamp = datetime.now().isoformat()
        
        command_data = {
            'timestamp': timestamp,
            'cmd_start': int(cmd_start),
            'cmd_stop': int(cmd_stop),
            'cmd_estop': int(cmd_estop),
            'processed': False  # Flag para que TIA Portal lo marque como procesado
        }
        
        if sp_ref_cm is not None:
            command_data['sp_ref_cm'] = float(sp_ref_cm)
        
        # Push crea un ID único automáticamente
        new_ref = ref.push(command_data)
        return new_ref.key
    except Exception as e:
        st.error(f"Error al insertar comando: {e}")
        return None

def insert_event(event_type, details):
    """Registra eventos en Firebase"""
    try:
        ref = db.reference('event_log')
        timestamp = datetime.now().isoformat()
        
        event_data = {
            'timestamp': timestamp,
            'event_type': event_type,
            'details': details
        }
        
        ref.push(event_data)
    except Exception as e:
        st.error(f"Error al insertar evento: {e}")

def get_latest_telemetry(n_rows=200):
    """Obtiene los últimos datos de telemetría desde Firebase"""
    try:
        ref = db.reference('telemetry_samples')
        
        # Obtener los últimos n_rows registros
        data = ref.order_by_key().limit_to_last(n_rows).get()
        
        if data:
            # Convertir a DataFrame
            records = []
            for key, value in data.items():
                record = value.copy()
                record['key'] = key
                records.append(record)
            
            df = pd.DataFrame(records)
            
            # Convertir timestamp a datetime si existe
            if 'timestamp' in df.columns:
                df['ts'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('ts')
            
            # Asegurar que existan las columnas esperadas
            expected_cols = ['level_cm', 'vfd_rpm', 'vfd_speedcmd', 'blink_2hz', 
                           'reached_sp', 'low_level', 'high_level']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = 0
            
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener telemetría: {e}")
        return pd.DataFrame()

def get_recent_events(n_rows=50):
    """Obtiene los eventos recientes desde Firebase"""
    try:
        ref = db.reference('event_log')
        
        # Obtener los últimos n_rows eventos
        data = ref.order_by_key().limit_to_last(n_rows).get()
        
        if data:
            records = []
            for key, value in data.items():
                record = value.copy()
                record['key'] = key
                records.append(record)
            
            df = pd.DataFrame(records)
            
            # Convertir timestamp a datetime
            if 'timestamp' in df.columns:
                df['ts'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('ts', ascending=False)
            
            # Renombrar columnas si es necesario
            if 'event_type' not in df.columns and 'type' in df.columns:
                df = df.rename(columns={'type': 'event_type'})
            
            return df[['ts', 'event_type', 'details']].head(n_rows)
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener eventos: {e}")
        return pd.DataFrame()

def get_current_status():
    """Obtiene el estado actual del sistema"""
    try:
        ref = db.reference('current_status')
        status = ref.get()
        return status if status else {}
    except Exception as e:
        st.error(f"Error al obtener estado: {e}")
        return {}

# ----------------- UI de Streamlit -----------------
st.set_page_config(page_title="SCADA en la Nube - Firebase", layout="wide")
st.title("☁️ Supervisión en la Nube con Firebase")

# Información de conexión
with st.expander("ℹ️ Información de conexión"):
    st.write("""
    **Estructura de Firebase Realtime Database:**
    - `/control_commands/` - Comandos enviados desde Streamlit a TIA Portal
    - `/telemetry_samples/` - Datos de telemetría desde TIA Portal (nivel, VFD, etc.)
    - `/event_log/` - Registro de eventos del sistema
    - `/current_status/` - Estado actual del sistema
    """)

left, right = st.columns([1, 2])

with left:
    st.subheader("Referencia (cm)")
    col1, col2 = st.columns(2)
    
    with col1:
        sp_slider = st.slider("Control deslizante", 0, 100, 50, 1, key="sp_slider")
    
    with col2:
        sp_text = st.text_input("Caja de texto", value=str(sp_slider), key="sp_text")
    
    # Validar entrada de texto
    try:
        sp_text_val = float(sp_text)
        sp_text_val = max(0.0, min(100.0, sp_text_val))
    except:
        sp_text_val = float(sp_slider)
    
    # Sincronizar slider con texto
    if abs(sp_text_val - float(sp_slider)) > 1e-6:
        st.session_state.sp_slider = int(round(sp_text_val))
    
    if st.button("✅ Enviar referencia", use_container_width=True):
        cmd_id = insert_command(sp_ref_cm=float(st.session_state.sp_slider))
        insert_event("SETPOINT_CHANGE", f"sp_ref_cm={st.session_state.sp_slider}")
        st.success(f"✓ Referencia enviada: {st.session_state.sp_slider} cm")
        st.caption(f"ID: {cmd_id}")
    
    st.divider()
    st.subheader("Comandos")
    
    c1, c2, c3 = st.columns(3)
    
    with c1:
        if st.button("▶️ Start", use_container_width=True):
            cmd_id = insert_command(cmd_start=1)
            insert_event("START", "Start")
            st.success("Start enviado")
    
    with c2:
        if st.button("⏹ Stop", use_container_width=True):
            cmd_id = insert_command(cmd_stop=1)
            insert_event("STOP", "Stop")
            st.warning("Stop enviado")
    
    with c3:
        if st.button("🛑 E-Stop", use_container_width=True):
            cmd_id = insert_command(cmd_estop=1)
            insert_event("ESTOP", "Paro de emergencia")
            st.error("¡E-Stop!")
    
    st.caption("La app escribe comandos en Firebase. El gateway PLC los lee y publica telemetría.")
    
    # Botón de actualización
    st.divider()
    if st.button("🔄 Actualizar datos", use_container_width=True):
        st.rerun()

with right:
    st.subheader("Estado del Proceso")
    
    # Obtener datos de telemetría
    df = get_latest_telemetry(200)
    
    if df.empty:
        st.info("⏳ Sin datos aún en Firebase (telemetry_samples). Cuando el gateway publique desde TIA Portal, verás valores y curvas.")
        st.markdown("""
        **Estructura esperada en Firebase:**
        ```json
        {
          "telemetry_samples": {
            "sample_id": {
              "timestamp": "2025-01-17T10:30:00",
              "level_cm": 45.5,
              "vfd_rpm": 1200,
              "vfd_speedcmd": 50,
              "blink_2hz": 1,
              "reached_sp": 0,
              "low_level": 0,
              "high_level": 0
            }
          }
        }
        ```
        """)
    else:
        # Mostrar métricas del último registro
        latest = df.iloc[-1]
        
        cA, cB, cC, cD = st.columns(4)
        
        with cA:
            st.metric("Nivel", f"{latest['level_cm']:.1f} cm")
        
        with cB:
            st.metric("VFD RPM", f"{latest['vfd_rpm']:.0f} rpm")
        
        with cC:
            blink_status = "🟢" if int(latest.get("blink_2hz", 0)) == 1 else "⚪"
            st.write("Parpadeo 2 Hz:")
            st.write(blink_status)
        
        with cD:
            sp_status = "✅" if int(latest.get("reached_sp", 0)) == 1 else "—"
            st.write("Alcanzó SP:")
            st.write(sp_status)
        
        # Alarmas de seguridad
        if int(latest.get("low_level", 0)) == 1:
            st.error("⚠️ ALARMA: Nivel bajo activado")
        if int(latest.get("high_level", 0)) == 1:
            st.error("⚠️ ALARMA: Nivel alto activado")
        
        st.divider()
        
        # Gráficas
        g1, g2 = st.columns(2)
        
        with g1:
            st.write("**Nivel (cm)**")
            st.line_chart(df.set_index("ts")[["level_cm"]])
        
        with g2:
            st.write("**VFD RPM**")
            st.line_chart(df.set_index("ts")[["vfd_rpm"]])
    
    st.divider()
    st.subheader("Eventos Recientes")
    
    ev = get_recent_events(50)
    
    if not ev.empty:
        st.dataframe(ev, use_container_width=True, hide_index=True)
    else:
        st.write("Sin eventos registrados.")

# Footer con información de auto-refresco
st.divider()
st.caption("💡 Presiona 'Actualizar datos' para ver los cambios más recientes desde TIA Portal")



