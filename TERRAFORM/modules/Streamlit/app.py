import streamlit as st
import pandas as pd
import json
import os
from google.cloud import pubsub_v1, bigquery
import pydeck as pdk
import db_dtypes

# Configuración de Google Cloud a partir de variables de entorno
PROJECT_ID = os.environ.get("PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE_UNMATCHED = os.environ.get("BQ_TABLE_UNMATCHED")
BQ_TABLE_MATCHED = os.environ.get("BQ_TABLE_MATCHED")

# Configurar clientes de Google Cloud
publisher = pubsub_v1.PublisherClient()
bq_client = bigquery.Client()

# Tópicos de Pub/Sub desde variables de entorno
TOPIC_AFFECTED_ENV = os.environ.get("AFFECTED_TOPIC")
TOPIC_VOLUNTEER_ENV = os.environ.get("VOLUNTEER_TOPIC")

TOPIC_AFFECTED = f"projects/{PROJECT_ID}/topics/{TOPIC_AFFECTED_ENV}"
TOPIC_VOLUNTEER = f"projects/{PROJECT_ID}/topics/{TOPIC_VOLUNTEER_ENV}"

# Validar que las variables de entorno están configuradas correctamente
if not all([PROJECT_ID, BQ_DATASET, BQ_TABLE_UNMATCHED, BQ_TABLE_MATCHED, TOPIC_AFFECTED, TOPIC_VOLUNTEER]):
    st.error("Faltan variables de entorno necesarias. Verifique la configuración.")
    st.stop()

# Crear pestañas en Streamlit
tab1, tab2, tab3 = st.tabs(["Registro", "Mapa de Alertas", "Búsqueda por Teléfono"])

# Pestaña 1: Registro
with tab1:
    st.title("Simulación de Plataforma de Ayuda")
    user_type = st.radio("¿Eres afectado o voluntario?", ("Afectado", "Voluntario"))

    if user_type == "Afectado":
        st.subheader("Registro de Afectado")
        location = st.selectbox("Localización", ["", "Albal", "Paiporta", "Torrent", "Aldaia"])
        need = st.selectbox("Necesidad", ["", "Supplies", "Cleaning", "Medical", "Transport"])
        availability = st.selectbox("Disponibilidad", ["", "Morning", "Afternoon", "Night"])
        if st.button("Enviar solicitud"):
            if not location or not need or not availability:
                st.warning("⚠️ Todos los campos deben estar seleccionados.")
            else:
                data = {"location": location, "need": need, "availability": availability}
                message = json.dumps(data).encode("utf-8")
                future = publisher.publish(TOPIC_AFFECTED, message)
                st.success("Solicitud enviada correctamente.")
                st.write(f"Mensaje publicado en Pub/Sub con ID: {future.result()}")

    elif user_type == "Voluntario":
        st.subheader("Registro de Voluntario")
        location = st.multiselect("Localización", ["Albal", "Paiporta", "Torrent", "Aldaia"])
        availability = st.multiselect("Disponibilidad", ["Morning", "Afternoon", "Night"])
        skills = st.multiselect("Capacidades", ["Supplies", "Cleaning", "Medical", "Transport"])
        if st.button("Registrarse como voluntario"):
            if not location or not availability or not skills:
                st.warning("⚠️ Debes seleccionar al menos una opción en cada campo.")
            else:
                data = {"location": location, "availability": availability, "skills": skills}
                message = json.dumps(data).encode("utf-8")
                future = publisher.publish(TOPIC_VOLUNTEER, message)
                st.success("Registro de voluntario enviado correctamente.")
                st.write(f"Mensaje publicado en Pub/Sub con ID: {future.result()}")

# Pestaña 2: Mapa de Alertas
with tab2:
    st.title("Mapa de Alertas no Matcheadas")

    # Consulta para obtener datos desde BigQuery (tabla unmatched)
    QUERY = f"""
    SELECT 
      type,
      timestamp,
      name,
      phone,
      category,
      message,
      necessity,
      city,
      disponibility,
      processed,
      affected_latitude,
      affected_longitude
    FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_UNMATCHED}`
    WHERE affected_latitude IS NOT NULL AND affected_longitude IS NOT NULL
    """
    try:
        df = bq_client.query(QUERY).to_dataframe()
        if df.empty:
            st.warning("No hay alertas activas en este momento.")
        else:
            view_state = pdk.ViewState(
                latitude=df["affected_latitude"].mean(),
                longitude=df["affected_longitude"].mean(),
                zoom=10,
                min_zoom=5,
                max_zoom=15,
                pitch=0,
            )
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                df,
                get_position=["affected_longitude", "affected_latitude"],
                get_fill_color=[255, 0, 0, 160],
                get_radius=200,
                pickable=True,
            )
            tooltip = {
                "html": """
                <b>Tipo:</b> {type} <br>
                <b>Fecha:</b> {timestamp} <br>
                <b>Nombre:</b> {name} <br>
                <b>Teléfono:</b> {phone} <br>
                <b>Categoría:</b> {category} <br>
                <b>Mensaje:</b> {message} <br>
                <b>Necesidad:</b> {necessity} <br>
                <b>Ciudad:</b> {city} <br>
                <b>Disponibilidad:</b> {disponibility} <br>
                <b>Procesado:</b> {processed}
                """,
                "style": {"backgroundColor": "white", "color": "black"}
            }
            st.pydeck_chart(pdk.Deck(layers=[scatter_layer], initial_view_state=view_state, tooltip=tooltip))
            st.subheader("Detalles de las alertas")
            st.dataframe(df)
    except Exception as e:
        st.error(f"Error al obtener datos de BigQuery: {e}")

# Pestaña 3: Búsqueda por Teléfono
with tab3:
    st.title("Buscar Petición por Teléfono")
    phone_input = st.text_input("Ingresa el número de teléfono (formato +34-699999999):")
    if st.button("Buscar"):
        if not phone_input:
            st.warning("Debes ingresar un número de teléfono.")
        else:
            # Query para tabla matched
            QUERY_MATCHED = f"""
            SELECT *
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_MATCHED}`
            WHERE affected_phone = '{phone_input}' OR volunteer_phone = '{phone_input}'
            """
            # Query para tabla unmatched
            QUERY_UNMATCHED = f"""
            SELECT *
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_UNMATCHED}`
            WHERE phone = '{phone_input}'
            """
            try:
                df_matched = bq_client.query(QUERY_MATCHED).to_dataframe()
            except Exception as e:
                st.error(f"Error al consultar la tabla matched: {e}")
                df_matched = pd.DataFrame()

            try:
                df_unmatched = bq_client.query(QUERY_UNMATCHED).to_dataframe()
            except Exception as e:
                st.error(f"Error al consultar la tabla unmatched: {e}")
                df_unmatched = pd.DataFrame()

            if df_matched.empty and df_unmatched.empty:
                st.info("No se encontró información para ese número de teléfono.")
            else:
                if not df_matched.empty:
                    st.subheader("Resultados en Matched")
                    st.dataframe(df_matched)
                if not df_unmatched.empty:
                    st.subheader("Resultados en Unmatched")
                    st.dataframe(df_unmatched)
