# LOCAL_firestore.py (basado en tu LOCAL.py)
import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

from firestore_db import get_firestore_client, insert_command_firestore, insert_event_firestore, get_latest_telemetry_firestore, get_recent_events_firestore

# --- Inicializar cliente Firestore ---
client = get_firestore_client()

# --- UI similar a tu LOCAL.py ---
st.set_page_config(page_title="Supervisión LOCAL (Firestore)", layout="wide")
st.title("🛠️ Supervisión LOCAL")

left, right = st.columns([1,2])

with left:
    st.subheader("Referencia (cm)")
    col1, col2 = st.columns(2)
    with col1:
        sp_slider = st.slider("Slider", 0, 100, 50, 1, key="sp_slider")
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
        insert_command_firestore(client, sp_ref_cm=float(st.session_state.sp_slider))
        insert_event_firestore(client, "SETPOINT_CHANGE", f"sp_ref_cm={st.session_state.sp_slider}")
        st.success(f"Referencia enviada: {st.session_state.sp_slider} cm")

    st.divider()
    st.subheader("Comandos")
    c1,c2,c3 = st.columns(3)
    with c1:
        if st.button("▶️ Start"):
            insert_command_firestore(client, cmd_start=1); insert_event_firestore(client, "START","Start"); st.success("Start enviado")
    with c2:
        if st.button("⏹ Stop"):
            insert_command_firestore(client, cmd_stop=1); insert_event_firestore(client, "STOP","Stop"); st.warning("Stop enviado")
    with c3:
        if st.button("🛑 E-Stop"):
            insert_command_firestore(client, cmd_estop=1); insert_event_firestore(client, "ESTOP","Paro de emergencia"); st.error("¡E-Stop!")

    if st.button("🔄 Refrescar datos"):
        st.experimental_rerun()

with right:
    st.subheader("Estado")
    df = get_latest_telemetry_firestore(client, 200)
    if df.empty:
        st.info("Sin datos aún en telemetry_samples.")
    else:
        latest = df.iloc[-1]
        cA, cB, cC, cD = st.columns(4)
        with cA: st.metric("Level_cm", f"{latest['level_cm']:.1f} cm")
        with cB: st.metric("VFD_RPM", f"{latest['vfd_rpm']:.0f} rpm")
        with cC: st.write("Blink 2Hz:", "🟢" if int(latest["blink_2hz"])==1 else "⚪")
        with cD: st.write("Reached SP:", "✅" if int(latest["reached_sp"])==1 else "—")
        st.divider()
        g1, g2 = st.columns(2)
        with g1: st.line_chart(df.set_index("ts")[["level_cm"]])
        with g2: st.line_chart(df.set_index("ts")[["vfd_rpm"]])

    st.divider()
    st.subheader("Eventos recientes")
    ev = get_recent_events_firestore(client, 50)
    if not ev.empty:
        st.dataframe(ev, use_container_width=True, hide_index=True)
    else:
        st.write("Sin eventos.")
