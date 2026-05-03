# 🚀 CryptoStream: Arquitectura de Monitoreo de Activos en Tiempo Real

## 📋 Descripción del Proyecto
Este proyecto nace de la necesidad de procesar grandes volúmenes de datos financieros con latencia mínima. Se trata de un ecosistema completo basado en **Big Data** que captura, procesa, almacena y visualiza el precio de criptomonedas (como BTC/USDT) de forma automática.

A diferencia de un sistema tradicional, esta arquitectura utiliza un enfoque de **Sistemas Distribuidos**, lo que permite que el pipeline sea escalable y resistente a fallos. Si un componente cae, el sistema tiene la capacidad de recuperarse sin perder la integridad de los datos.

---

## 🏗️ ¿Cómo funciona? (La Arquitectura)

El flujo de los datos sigue un camino de cuatro etapas principales:

1.  **Captura (Ingesta):** Un productor desarrollado en Python se conecta a la API de Binance para obtener actualizaciones de precios cada segundo. Estos datos se envían a un clúster de **Apache Kafka**, que actúa como un "amortiguador" para asegurar que ningún dato se pierda.
2.  **Procesamiento (Streaming):** **Apache Spark** lee el flujo de Kafka en tiempo real. Aquí se realizan transformaciones de datos (limpieza y tipado) y se calculan métricas de negocio.
3.  **Persistencia (Almacenamiento):** Los datos procesados se guardan de forma permanente en **HDFS** (Hadoop Distributed File System) utilizando el formato **Parquet**, optimizado para consultas analíticas pesadas.
4.  **Observabilidad (Visualización):** El sistema expone su salud interna a través de **Prometheus**, y finalmente, un dashboard en **Grafana** muestra gráficas interactivas del mercado y del rendimiento del sistema.

---

## 🛠️ Tecnologías Utilizadas

*   **Docker & Docker Compose:** Para garantizar que el proyecto funcione en cualquier ordenador mediante contenedores.
*   **Apache Kafka:** El motor de mensajería de alta disponibilidad.
*   **Apache Spark (v3.0.1):** El cerebro que procesa los datos en paralelo.
*   **Hadoop (HDFS):** El sistema de archivos donde se guarda la historia de los precios.
*   **Prometheus:** El guardián que vigila las métricas y la salud del pipeline.
*   **Grafana:** La cara visible que transforma números en decisiones visuales.

---

## 🚀 Guía de Inicio Rápido

### Requisitos Previos
*   Tener instalado [Docker Desktop](https://www.docker.com/products/docker-desktop/).
*   Tener Python 3.x instalado en el equipo local.

### Paso 1: Levantar la Infraestructura
En la raíz del proyecto, ejecuta el comando que encenderá todos los servidores:
```bash
docker-compose up -d
