# DANA Solidaria · Máster Big Data & Cloud (EDEM)

En septiembre de 2024 la DANA que azotó Valencia dejó a miles de personas incomunicadas o sin recursos básicos.  
Nuestro reto como equipo del máster fue **conectar rápidamente a afectados con voluntarios** usando solo servicios gestionados de Google Cloud y desplegándolo todo con Terraform.

---

## 1. El problema en dos frases

1. **Afectados** publicaban necesidades urgentes (evacuación, comida, alojamiento…).  
2. **Voluntarios** ofrecían ayuda, pero localizar el mejor emparejamiento manualmente era lento y propenso a errores.

---

## 2. Enfoque cloud-native

| Necesidad                                | Servicio de Google que usamos | Motivo de la elección                                         |
|------------------------------------------|-------------------------------|---------------------------------------------------------------|
| Capturar cada alta en tiempo real        | **Pub/Sub**                   | Cola de eventos escalable y fiable.                           |
| Procesar lógica de emparejamiento        | **Cloud Functions**           | Ejecuta código bajo demanda sin gestionar servidores.         |
| Almacenar y analizar los eventos         | **BigQuery**                  | SQL serverless, particiones por fecha, consultas en segundos. |
| Transformar y asegurar calidad de datos  | **Dataform**                  | ELT declarativo con control de versiones y tests.             |
| Exponer API a la web                     | **Cloud Run (Streamlit)**     | Contenedores auto-escalables con pay-per-use.                 |
| Autenticación de usuarios                | **Firebase Auth**             | Registro social/email en minutos.                             |
| Despliegue reproducible                  | **Terraform + Cloud Build**   | Infraestructura como código y CI/CD.                          |

---

## 3. Resultado

- **Asignación automática en segundos** (antes: manual, minutos u horas).  
- **Escala horizontal gratis** hasta el tráfico que se necesite gracias a los triggers de Pub/Sub y Cloud Run.  
- **Coste menor a 2 €/día** durante la prueba de concepto (cuotas gratuitas de GCP).  
- **Cero servidores que mantener**; todo es serverless o completamente gestionado.

---

## 4. Cómo reutilizar la idea

1. Clona el repo: 
2. Crea un proyecto GCP y ejecuta `terraform apply`.  



