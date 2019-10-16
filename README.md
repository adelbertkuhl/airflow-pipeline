# airflow-pipeline

I've set up a basic Airflow data pipeline that runs in the Google Cloud Composer Environment. 

- The pipeline performs REST API requests to the [IEX Cloud financial data platform](https://iexcloud.io/) and retrieves current exchange rates for multiple cryptocurrencies every 30 seconds. 
- Airflow executes an operation that cleans the response JSON and exports the exchange rate data for each currency to CSV files in the environment-associated GCS bucket (the `/data/` folder). 
- The `GoogleCloudStorageToBigQueryOperator` tool loads the CSVs from the GCS bucket to their respective BigQuery tables (creating them if they don't already exist).

# detailed workflow


