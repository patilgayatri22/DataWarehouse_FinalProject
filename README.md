# DataWarehouse_FinalProject
Paroject Group : 5
Deeksha Chauhan, Gayatri Patil, Nikhil Swami, Shweta Shinde

The project utilizes Snowflake for data storage, Apache Airflow for orchestration, dbt for data transformation, and Tableau for visualization, leveraging each tool's strengths to create a comprehensive analytics solution. Weather data is crucial for various sectors, including agriculture, energy, transportation, and urban planning. This project aims to create an efficient, scalable data pipeline that can ingest, process, and visualize weather data, providing valuable insights for decision-making.

**Objectives:**
1. Implement an ETL process to populate raw weather data tables in Snowflake using Airflow
1. Develop an ELT process using dbt to create abstract tables with calculated weather metrics
2. 3. Visualize key weather analytics using Tableau
Integrate all components into a cohesive, automated pipeline

**Specification:**

Step 1: Data Source:
• The pipeline starts with a data source API (Visual Crossings Weather Data Services ), likely fetching weather data for California region on daily basis.
Historical data - Jan 2020 to Oct 2024
Real Time API Data - Nov 1 to till date (daily Dag running)
Step 2: Data Ingestion:
• DAG 1: This ELT Pipeline is responsible for loading the fetched data into a Snowflake table.
Step 3: Data Transformation
• DAG 2: Data is processed and enriched in ELT pipeline with transformations and analysis like temperature_range, daily_changes, daylight_duration, solar_efficiency, pressure_drops, cumulative_precipitation using DBT and Docker.
Step 4: Data Visualization:
• Tableau is used for visualizing weather trends and daily change analysis of weather.

**Python Files Overview**

1.csv_historical_data_etl.py
Purpose: Extracts historical weather data from an external API into CSV file and then loads into Snowflake table. Ran multiple times due to restrictions of API to collect larger amount of data from Jan 2020 to Oct 2024.
Key Functions:
process_csv(): Fetches weather data from API
upload_to_snowflake(): 

2. project_ETL_pipeline1.py
Purpose: Extracts weather data from an external API and loads it into Snowflake.
Key Functions:
return_weatherdata(): Fetches current weather price data.
create_load_incremental(): Loads the extracted data into a Snowflake staging table and then main table(incremental load).

3. project_elt_dbt.py
Purpose: Perform advance transformations and analysis like temperature_range, daily_changes, daylight_duration, solar_efficiency, pressure_drops, cumulative_precipitation using DBT.
Key Functions:
dbt run: The bash operator will run dbt run on command line to trigger the calcualtion on snowflake and create 6 new tables.
dbt snapshot: The bash operator will run dbt snapshot on command line and create a snapshot of the given table.
dbt test: The bash operator will run dbt test on command line to perform given test on the given table.

4. proj
Purpose: dbt folder with holds the code to perform advance calculation.
Key Functions:
models: It contains Input Output folders which has CTE codes to apply tranformations on input model.
snapshot: It conatian the detials about the tables of which we need to take snapshot.
test: It conatian the detials about the tables of which we perform test like "not null", "unique"

5. California_Weather.twb & History_weather.twb
Purpose: To plot graphs on tableau using the data loading in the tables.
Plots:
Area Gragh
Bar Plot
Bar and Line Plot


**Conclusion**
This project demonstrates the successful integration of modern data tools to create an end-to-end analytics pipeline for weather data. By leveraging Snowflake, Airflow, dbt, and Tableau, we've created a scalable, maintainable solution that can provide valuable insights for weather analysis and its impact on various sectors.
The modular nature of the pipeline allows for easy expansion and modification, making it adaptable to changing requirements and additional data sources. This approach to data analytics can be applied to various domains beyond weather analysis, showcasing the versatility of the chosen tools and architecture.

**Acknowledgments**
Apache Airflow for orchestration.
Snowflake for data warehousing capabilities.
