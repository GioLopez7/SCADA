import os
import pandas as pd
import streamlit as st
import pymysql as mysql


# ----------------- Config: variables de entorno -----------------
# En Streamlit Cloud: vendrán de .streamlit/secrets.toml
# En Render: vendrán de Environment Variables
DB_HOST = os.getenv("DB_HOST") or st.secrets["DB_HOST"]
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER") or st.secrets["DB_USER"]
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets["DB_PASSWORD"]
DB_NAME = os.getenv("DB_NAME") or st.secrets["DB_NAME"]

def get_conn():
    return mysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

# ----------------- Acceso a BD -----------------
def insert_command(cmd_start=0, cmd_stop=0, cmd_estop=0, sp_ref_cm=None):
    conn = get_conn(); cur = conn.cursor()
    if sp_ref_cm is None:
        cur.execute(
            "INSERT INTO control_commands (cmd_start, cmd_stop, cmd_estop) VALUES (%s,%s,%s)",
            (int(cmd_start), int(cmd_stop), int(cmd_estop)),
        )
    else:
        cur.execute(
            "INSERT INTO control_commands (cmd_start, cmd_stop, cmd_estop, sp_ref_cm) VALUES (%s,%s,%s,%s)",
            (int(cmd_start), int(cmd_stop), int(cmd_estop), float(sp_ref_cm)),
        )
    conn.commit(); cur.close(); conn.close()

def insert_event(event_type, details):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO event_log (event_type, details) VALUES (%s,%s)", (event_type, details))
    conn.commit(); cur.close(); conn.close()

def get_latest_telemetry(n_rows=200):
    conn = get_conn()
    df = pd.read_sql(
        f"""
        SELECT ts, level_cm, vfd_rpm, vfd_speedcmd, blink_2hz, reached_sp, low_level, high_level
        FROM telemetry_samples
        ORDER BY ts DESC
        LIMIT {int(n_rows)}
        """, conn
    )
    conn.close()
    if not df.empty:
        df = df.sort_values("ts")
    return df

def get_recent_events(n_rows=50):
    conn = get_conn()
    df = pd.read_sql(
        f"SELECT ts, event_type, details FROM event_log ORDER BY ts DESC LIMIT {int(n_rows)}",
        conn
    )
    conn.close()
    return df

# ----------------- UI -----------------
st.set_page_config(page_title="SCADA en la Nube", layout="wide")
st.title("☁️ Supervisión en la Nube – Laboratorio de Automatización")

left, right = st.columns([1,2])

with left:
    st.subheader("Referencia (cm)")
    col1, col2 = st.columns(2)
    with col1:
        sp_slider = st.slider("Control deslizante", 0, 100, 50, 1, key="sp_slider")
    with col2:
        sp_text = st.text_input("Caja de texto", value=str(sp_slider), key="sp_text")
    try:
        sp_text_val = float(sp_text)
        sp_text_val = max(0.0, min(100.0, sp_text_val))
    except:
        sp_text_val = float(sp_slider)
    if abs(sp_text_val - float(sp_slider)) > 1e-6:
        st.session_state.sp_slider = int(round(sp_text_val))

    if st.button("✅ Enviar referencia"):
        insert_command(sp_ref_cm=float(st.session_state.sp_slider))
        insert_event("SETPOINT_CHANGE", f"sp_ref_cm={st.session_state.sp_slider}")
        st.success(f"Referencia enviada: {st.session_state.sp_slider} cm")

    st.divider()
    st.subheader("Comandos")
    c1,c2,c3 = st.columns(3)
    with c1:
        if st.button("▶️ Start"):
            insert_command(cmd_start=1); insert_event("START","Start"); st.success("Start enviado")
    with c2:
        if st.button("⏹ Stop"):
            insert_command(cmd_stop=1); insert_event("STOP","Stop"); st.warning("Stop enviado")
    with c3:
        if st.button("🛑 E-Stop"):
            insert_command(cmd_estop=1); insert_event("ESTOP","Paro de emergencia"); st.error("¡E-Stop!")

    st.caption("La app escribe comandos en la BD cloud. El gateway PLC los lee y publica telemetría.")

with right:
    st.subheader("Estado")
    df = get_latest_telemetry(200)
    if df.empty:
        st.info("Sin datos aún en la BD cloud (telemetry_samples). Cuando el gateway publique, verás valores y curvas.")
    else:
        latest = df.iloc[-1]
        cA, cB, cC, cD = st.columns(4)
        with cA: st.metric("Nivel_cm", f"{latest['level_cm']:.1f} cm")
        with cB: st.metric("VFD_RPM", f"{latest['vfd_rpm']:.0f} rpm")
        with cC: st.write("Parpadeo 2 Hz:", "🟢" if int(latest["blink_2hz"])==1 else "⚪")
        with cD: st.write("Alcanzó SP:", "✅" if int(latest["reached_sp"])==1 else "—")
        st.divider()
        g1, g2 = st.columns(2)
        with g1: st.line_chart(df.set_index("ts")[["level_cm"]])
        with g2: st.line_chart(df.set_index("ts")[["vfd_rpm"]])

    st.divider()
    st.subheader("Eventos recientes")
    ev = get_recent_events(50)
    if not ev.empty:
        st.dataframe(ev, use_container_width=True, hide_index=True)
    else:
        st.write("Sin eventos.")
