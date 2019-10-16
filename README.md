# airflow-pipeline

I've set up a basic Airflow data pipeline that runs in the Google Cloud Composer Environment. 

- The pipeline performs REST API requests to the [IEX Cloud financial data platform](https://iexcloud.io/) and retrieves current exchange rates for multiple cryptocurrencies every 30 seconds. 
- Airflow executes an operation that cleans the response JSON and exports the exchange rate data for each currency to CSV files in the environment-associated GCS bucket (the `/data/` folder). 
- The `GoogleCloudStorageToBigQueryOperator` tool loads the CSVs from the GCS bucket to their respective BigQuery tables (creating them if they don't already exist).

The Cloud Composer Airflow console shows the status of each task in the pipeline:
![Image description](https://github.com/adelbertkuhl/airflow-pipeline/blob/master/img/Screen%20Shot%202019-10-15%20at%2011.33.12%20PM.png)

We can run a simple query against one of the resulting BigQuery tables to current price data. 
```
select * from `airflow-pipeline-777216.airflow_pipeline_dataset.price_data_LTCUSDT` ;
```
![Image description](https://github.com/adelbertkuhl/airflow-pipeline/blob/master/img/Screen%20Shot%202019-10-15%20at%209.56.38%20PM.png)

