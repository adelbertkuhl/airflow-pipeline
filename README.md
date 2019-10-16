# airflow-pipeline

I've set up a basic Airflow data pipeline that runs in the Google Cloud Composer Environment. The pipeline performs REST API requests to the IEX financial data warehouse and retrieves current exchange rates for multiple cryptocurrencies every 30 seconds. Airflow cleans the response JSON and dumps the exchange rate data for each currency in the associated GCS bucket `/data/` folder. The `GoogleCloudStorageToBigQueryOperator` tool loads the CSVs from the GCS bucket to their respective BigQuery tables (creating them if they don't already exist).

