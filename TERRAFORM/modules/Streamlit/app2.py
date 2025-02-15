import streamlit as st
import pandas as pd
import json
from google.cloud import pubsub_v1, bigquery
import pydeck as pdk

# Configuración de Google Cloud
PROJECT_ID = "data-project-2425"
BQ_DATASET = "terreta_data"
BQ_TABLE = "non_matched_table"

# Configurar clientes de Google Cloud
publisher = pubsub_v1.PublisherClient()
bq_client = bigquery.Client()

# Tópicos de Pub/Sub
TOPIC_AFFECTED = f"projects/{PROJECT_ID}/topics/affected"
TOPIC_VOLUNTEER = f"projects/{PROJECT_ID}/topics/volunteer"

# Crear pestañas en Streamlit
tab1, tab2 = st.tabs(["Registro", "Mapa de Alertas"])

# Pestaña 1: Registro
with tab1:
    st.title("Simulación de Plataforma de Ayuda")

    # Elección de usuario
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
                data = {
                    "location": location,
                    "need": need,
                    "availability": availability
                }
                message = json.dumps(data).encode("utf-8")
                future = publisher.publish(TOPIC_AFFECTED, message)
                st.success("Solicitud enviada correctamente.")
                st.write(f"Mensaje publicado en Pub/Sub con ID: {future.result()}")  # Depuración

    elif user_type == "Voluntario":
        st.subheader("Registro de Voluntario")

        location = st.multiselect("Localización", ["Albal", "Paiporta", "Torrent", "Aldaia"])
        availability = st.multiselect("Disponibilidad", ["Morning", "Afternoon", "Night"])
        skills = st.multiselect("Capacidades", ["Supplies", "Cleaning", "Medical", "Transport"])

        if st.button("Registrarse como voluntario"):
            if not location or not availability or not skills:
                st.warning("⚠️ Debes seleccionar al menos una opción en cada campo.")
            else:
                data = {
                    "location": location,
                    "availability": availability,
                    "skills": skills
                }
                message = json.dumps(data).encode("utf-8")
                future = publisher.publish(TOPIC_VOLUNTEER, message)
                st.success("Registro de voluntario enviado correctamente.")
                st.write(f"Mensaje publicado en Pub/Sub con ID: {future.result()}")  # Depuración

# Pestaña 2: Mapa de Alertas
with tab2:
    st.title("Mapa de Alertas no Matcheadas")

    # Query para obtener datos desde BigQuery
    QUERY = f"""
    SELECT city, affected_latitude, affected_longitude, necessity, message, timestamp
    FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE affected_latitude IS NOT NULL AND affected_longitude IS NOT NULL
    """

    try:
        df = bq_client.query(QUERY).to_dataframe()

        if df.empty:
            st.warning("No hay alertas activas en este momento.")
        else:
            import pydeck as pdk

            # Estado inicial del mapa
            view_state = pdk.ViewState(
                latitude=df["affected_latitude"].mean(),
                longitude=df["affected_longitude"].mean(),
                zoom=10,  # Zoom inicial
                min_zoom=5,  # Zoom mínimo permitido
                max_zoom=15,  # Zoom máximo permitido
                pitch=0,
            )

            # Capa de puntos individuales con tamaño dinámico
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                df,
                get_position=["affected_longitude", "affected_latitude"],
                get_fill_color=[255, 0, 0, 160],  # Rojo semi-transparente
                get_radius="1000 / viewState.zoom",  # Ajuste dinámico del tamaño
                pickable=True,
            )

            # Tooltip para mostrar información al pasar el cursor
            tooltip = {
                "html": "<b>Ciudad:</b> {city} <br> <b>Tipo de alerta:</b> {necessity} <br> <b>Mensaje:</b> {message}",
                "style": {"backgroundColor": "white", "color": "black"},
            }

            # Mostrar el mapa en Streamlit
            st.pydeck_chart(pdk.Deck(layers=[scatter_layer], initial_view_state=view_state, tooltip=tooltip))

            # Tabla con detalles de las alertas
            st.subheader("Detalles de las alertas")
            st.dataframe(df[["city", "necessity", "message", "timestamp"]])

    except Exception as e:
        st.error(f"Error al obtener datos de BigQuery: {e}")