import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
edw_schema = config['DWH_DB']['DB_SCHEMA_NAME']
stg_schema = config['STAGING_DB']['DB_SCHEMA_NAME']

drop_date_table = (""" drop table {dwh_schema}.dim_date;""")
date_table_create = ("""
create table if not exists {dwh_schema}.dim_date (
                                gregorian_date  date PRIMARY key,
                                day         integer,
                                week        integer,
                                month       integer,
                                year        integer                               
                                );
""").format(dwh_schema=edw_schema)

date_table_insert = ("""
create temp table stage (like {dwh_schema}.dim_date);

insert  
into
    stage
    ( SELECT distinct
        --(TIMESTAMP 'epoch' + ts * INTERVAL '0.001 Second ') start_time,
        gregorian_date,
        extract(day from gregorian_date) as "day",
        extract(week from gregorian_date) as "week",
        extract(month from gregorian_date) as "month",
        extract(year from gregorian_date) as "year"         
    FROM
        (
            select gregorian_date
            from 
                (
                    select distinct TO_DATE(arrival_date, 'YYYY-MM-DD') gregorian_date from {stg_schema}.i94 
                    union 
                    select distinct TO_DATE(departure_date, 'YYYY-MM-DD') from {stg_schema}.i94 
                ) x 
            where gregorian_date is not null 
        ) y
    );

begin transaction;        
delete from {dwh_schema}.dim_date t 
using stage 
where t.gregorian_date = stage.gregorian_date;

insert into {dwh_schema}.dim_date 
select * from stage;

end transaction;
drop table stage;            
""").format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dwh_tables = [drop_date_table]
create_dwh_tables = [date_table_create]
populate_dwh_tables = [date_table_insert]
