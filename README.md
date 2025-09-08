# Weather Batch Project

## Project Overview
This project automates the collection, processing, and visualization of weather data for **Berlin, Germany** using an automated batch workflow.

---

## Data Source  
Weather data is collected using the **Meteo API** by Meteosource, which provides hyperlocal and real-time weather data including temperature, wind, precipitation, humidity, pressure, and visibility.

---

## Data Storage  
- Raw and processed weather data are stored securely in a **Google Cloud Storage bucket**.  
- Data is loaded and managed using **Google BigQuery**, a scalable, fully managed cloud data warehouse.  
- Apache Spark performs data transformations coordinated with BigQuery.  
- Processed data powers Tableau visualizations published on **Tableau Public**.

---

## Key Technologies & Versions  
- Python 3.10  
- Apache Airflow 2.10.0  
- Apache Spark 3.4.1  
- Google BigQuery (managed service)  
- Tableau Desktop 2023.1+  
- Tableau Public (latest)  

---

## Project Structure

/airflow/ # Airflow DAGs and config
/dataflow/ # Spark processing scripts
/extraction/ # Extraction scripts and output data
/libs/ # Libraries and connectors
/my_env/ # Python virtual environment (excluded)
/spark-3.4.1-bin-hadoop3/ # Spark binaries (excluded)
/requirements.txt # Python dependencies
/.gitignore # Git ignore rules
/screenshots/ # Project screenshots
/sql/ # SQL query files
/README.md # Project documentation

---

## Setup & Usage  

1. Clone repository.  

2. Create and activate Python virtual environment:  
python -m venv my_env
source my_env/bin/activate # Or on Windows: my_env\Scripts\activate

3. Install dependencies:  
pip install -r requirements.txt

4. Configure Airflow, Spark, and environment variables.  

5. Run Airflow scheduler to execute DAGs.  

6. Use Tableau Desktop or Tableau Public dashboard for visualizations.

---

## Project Visuals

### Airflow UI
![Airflow UI](screenshots/airflow%20UI.png)

### BigQuery SQL Query
![BigQuery SQL](screenshots/bigquery%20sql.png)

### Google Cloud Storage Bucket
![GCS Bucket](screenshots/gcs%20bucket.png)

### Tableau Dashboard
![Tableau Dashboard](screenshots/tableau%20dashboard.png)

---

## Contributors  
- Baburam Marandi – [GitHub Profile](https://github.com/Baburam-bot?tab=repositories)