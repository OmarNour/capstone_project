## Project Summary:
-   **The Goal**: Build an analytics table for US immigrants
-   **Datasets**:   
    1.  I94 Immigration Data from: https://travel.trade.gov/research/reports/i94/historical/2016.html.
    2.  U.S. City Demographic Data from: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

-   **database layers**:
    1. **Staging layer**: this is almost one to one from source data to be able to transform and make join between tables easily.
    2.  **DWH layer**: here we have our data transformed data sored in a star schema, ready to answer questions.

## Files in the repository:
1. **dwh.cfg**: obtains configuration needed to populate the DWH db successfully
2. **sql_queries.py**: holds all create and insert sql statements we need to build the DWH db
3. **etl.py**: here we execute the statements we have writen in the sql_queries.py 
4. **data_quality.py**: contains all data quality rules to be applied

## How to run the python scripts:
1. Make sure configured dwh.cfg
2. run etl.py 

## Database Schema Design & ETL Pipeline:
-   **database layers**:
    1. **Staging layer**: this is almost one to one from the source to be able to transform and make join between tables easily.
    2.  **DWH layer**: here we have our data transformed ans sored in a star schema, ready to answer questions.
      
        - **Staging tables**:
            1. **i94**:  the table will be populated form SAS parquet files
            2. **countries**: data extracted manually for the label file 
            3. **gender**: data extracted manually for the label file
            4. **ports**: data extracted manually for the label file
            5. **port_modes**: data extracted manually for the label file
            5. **us_cities_demographics**: combined from the label file and the dataset #2 mentioned above
            6. **us_states**: data extracted manually for the label file
            7. **visa_categories**: data extracted manually for the label file
        - **Fact table**:
            1. **f_i94**: cleaned data populated from i94 table from the staging area   
        - **Dimension tables**:         
            *all tables below populated from the staging tables*
            1. **dim_airlines**
            2. **dim_countries**
            3. **dim_date**
            4. **dim_gender**
            5. **dim_port_modes**
            6. **dim_ports**
            7. **dim_us_states**
            8. **dim_visa_categories**
            9. **dim_visa_types**
        
## Running/loading to DWH steps:   
1.  read data from source to staging area
2.  run predefined quality rules on data in staging area
3.  load cleaned data from staging area to our DWH db.

## Addressing Other Scenarios:
-   **The data was increased by 100x:**:    
    DW to be moved to scalable DW in the cloud such as amazon redshift
-   **The pipelines would be run on a daily basis by 7 am every day:**  
    Airflow will handle our scheduling concerns   
-   **The database needed to be accessed by 100+ people:**  
    we need to define the common queries for so that we can choose the proper key to be used to distribute data among.
