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
            1. **staging_events**: a copy from event log files,  
                and because of the number of rows is small so we choose to make the distribution style "ALL".  
            2. **staging_songs**: a copy from song log files
        - **Fact table**:
            1. **songplays**:
        - **Dimension tables**:
            1. **users**:
            2. **songs**:
            3. **artists**:
            4. **time**
        

## Addressing Other Scenarios:
-   **The data was increased by 100x:** 
-   **The pipelines would be run on a daily basis by 7 am every day:**  
-   **The database needed to be accessed by 100+ people:**  
