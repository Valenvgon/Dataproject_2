import streamlit as st
from google.cloud import pubsub_v1
import json

# ID del proyecto en Google Cloud
PROJECT_ID = "data-project-2425"

# Nombres correctos de los topics
TOPIC_AFFECTED = f"projects/{PROJECT_ID}/topics/affected"
TOPIC_VOLUNTEER = f"projects/{PROJECT_ID}/topics/volunteer"

# Configurar cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()

# Títulos
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


# Ejecutar comando para autentificar conexion con Google Cloud $ gcloud auth application-default login
# Para instalar google.cloud hacer pip install google-cloud-pubsub