# --- pega esto al inicio de scada_cloud/app_cloud.py (reemplaza imports previos) ---
import streamlit as st
import pandas as pd
from datetime import datetime

# al principio del archivo scada_cloud/app_cloud.py
from firestore_db import (
    get_firestore_client,
    insert_command_firestore,
    insert_event_firestore,
    insert_telemetry_firestore,
    get_latest_telemetry_firestore,
    get_recent_events_firestore
)
# Inicializar Firestore (sin argumentos)
try:
    client = get_firestore_client()   # llamar SIN keyword args
except Exception as e:
    st.error("Error al inicializar Firebase: " + str(e))
    st.stop()

# --- resto de tu app sigue igual ---

st.set_page_config(page_title="Supervisión en la Nube", layout="wide")
st.title("☁️ Supervisión en la Nube - Laboratorio de Automatización")

# ---------- Inicializar cliente Firestore ----------
try:
    client = get_firestore_client(secret_name="firebase", env_name="FIREBASE_KEY_PATH")
except Exception as e:
    st.error("Error al inicializar Firebase: " + str(e))
    st.stop()

# ---------- Interfaz ----------
left, right = st.columns([1, 2])

with left:
    st.header("Referencia (cm)")
    col1, col2 = st.columns([2,1])
    with col1:
        sp = st.slider("Control deslizante", 0, 100, 50, key="sp_slider")
    with col2:
        sp_text = st.text_input("Caja de texto", value=str(sp), key="sp_text")
        # sincroniza si el usuario escribió manualmente
        try:
            sp_val = float(sp_text)
            if sp_val != st.session_state.sp_slider:
                st.session_state.sp_slider = int(round(sp_val))
                sp = st.session_state.sp_slider
        except:
            sp = st.session_state.sp_slider

    if st.button("✅ Enviar referencia"):
        insert_command(client, sp_ref_cm=float(sp))
        insert_event(client, "SETPOINT_CHANGE", f"sp_ref_cm={sp}")
        st.success(f"Referencia enviada: {sp} cm")

    st.markdown("---")
    st.header("Comandos")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("▶️ Inicio"):
            insert_command_firestore(client, cmd_start=1)
            insert_event_firestore(client, "CMD", "START")
            st.success("Inicio enviado")
    with c2:
        if st.button("⏹ Detener"):
            insert_command_firestore(client, cmd_stop=1)
            insert_event_firestore(client, "CMD", "STOP")
            st.warning("Stop enviado")
    with c3:
        if st.button("🛑 Parada de emergencia"):
            insert_command_firestore(client, cmd_estop=1)
            insert_event_firestore(client, "CMD", "ESTOP")
            st.error("E-STOP enviado")

    st.markdown("---")
    if st.button("🔄 Actualizar datos"):
        st.experimental_rerun()

with right:
    st.header("Estado")
    df = get_latest_telemetry_firestore(client, limit=200)

    if df.empty:
        st.info("Sin telemetría aún.")
    else:
        # show latest values
        latest = df.iloc[-1]
        a1, a2, a3, a4 = st.columns([2,2,1,1])
        a1.metric("Nivel_cm", f"{latest['level_cm']:.1f} cm")
        a2.metric("RPM del variador", f"{latest['vfd_rpm']:.0f} rpm")
        a3.write("Parpadeo 2 Hz:")
        a3.markdown("🟢" if int(latest["blink_2hz"])==1 else "⚪")
        a4.write("Alcanzado SP:")
        a4.markdown("✅" if int(latest["reached_sp"])==1 else "—")

        st.markdown("---")
        g1, g2 = st.columns(2)
        with g1:
            st.line_chart(df.set_index("ts")[["level_cm"]])
        with g2:
            st.line_chart(df.set_index("ts")[["vfd_rpm"]])

    st.markdown("---")
    st.header("Eventos recientes")
    ev = get_recent_events_firestore(client, 50)
    if ev.empty:
        st.write("Sin eventos.")
    else:
        st.dataframe(ev, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("Nota: la intermitencia 2Hz y el registro continuo de RPM/velocidades lo publica el gateway PLC. Esta app lee y muestra los datos.")









