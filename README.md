Overview
========

 A weather ETL (Extract, Transform, Load) pipeline built using Apache Airflow. The purpose of this pipeline is to fetch weather data for Lagos, Nigeria, transform it, and load it into a PostgreSQL database for storage and analysis.

Project Contents
================
The project consists of the following components:
#### 1. DAG Definition (weather_etl_pipeline):
   * This defines the workflow and schedules it to run daily.
   * it specifies default arguments like retries, retry delay and the start date.
#### 2. Tasks:
   * #### Extract(extract_weathera_data):
     * Fetches weather data from the Open-Meteo API for Lagos using HttpHook.
     * The API returns current weather information including temperature, wind speed and weather codes.
   * #### Transform(transform_weather_data):
     * Processes and transforms the raw weather data to extract meaningful fields(e.g., temperature, wind speed).
     * Organizes the data into a format ready for loading into the database.
   * #### Load(load_weather_data):
     * Connects to a PostgreSQL database using PostgresHook.
     * Creates a table (weather_data) to store the weather details if it doesn't exist.
     * Inserts the transformed data into the table.
#### 3. Connections:
   * API Connection: Managed through the Airflow connection open_meteo_api.
   * PostgreSQL Connection: Managed through the Airflow connection postgres_default.

Setup Instructions
==================
Prerequisites.
 * Ensure the following Airflow connections are configured:
    * **API Connection(open_meteo_api)**: Set up via Airflow's UI or CLI to store the API connection details.
     * **PostgreSQL Connection(postgres_default)**: Set up via Airflow's UI or CLI to store the PostgresSQL connection details.

### Clone the Repository

```bash
git clone https://github.com/BOLUF16/ETL_PIPELINE.git
cd ETL_PIPELINE
```
# Install Astro CLI
To install the Astro CLI, follow the instructions provided here.

[Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

# Initialize the Astro project:

```bash
astro dev init
```

# Start Airflow with Astro CLI
### Run the following command to start Airflow:

```bash
astro dev start
```




 
