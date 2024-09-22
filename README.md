# Requirements:
  Docker Compose >= 2.29.6

# Volumes
* ./input/ <- put there your data to aggregation as YYYY-MM-DD.csv files 
* ./output/ <- take from there your weekly aggregation data as YYYY-MM-DD.csv files 
* ./daily/ <- take from there your daily aggregation data as YYYY-MM-DD.csv files 

# Launch:
1. Run "docker compose up airflow-init"
2. Run "docker compose up"
3. Open Airflow UI in brawser by link "http://localhost:8080"
4. Search DAG called "spark_aggregation_dag"
5. Run it by pushin run button
6. Turn on DAG to run every day at 7:00 and aggregate 7 last days
