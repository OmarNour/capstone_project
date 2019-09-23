import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
edw_schema = config['DWH_DB']['DB_SCHEMA_NAME']
stg_schema = config['STAGING_DB']['DB_SCHEMA_NAME']

drop_dim_date = (""" drop table if exists {dwh_schema}.dim_date; """).format(dwh_schema=edw_schema)
create_dim_date = ("""
create table if not exists {dwh_schema}.dim_date (
                                gregorian_date  date PRIMARY key,
                                day         integer,
                                week        integer,
                                month       integer,
                                year        integer                               
                                );
""").format(dwh_schema=edw_schema)

populate_dim_date = ("""
create temp table stage (like {dwh_schema}.dim_date);

insert  
into
    stage
    ( SELECT distinct
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

drop_dim_countries = """ drop table if exists {dwh_schema}.dim_countries; """.format(dwh_schema=edw_schema)
create_dim_countries = """ create table if not exists {dwh_schema}.dim_countries(country_code   varchar(10), 
                                                                                country_Name    varchar(100)
                                                                                ); """.format(dwh_schema=edw_schema)

populate_dim_countries = """ 
create temp table stage (like {dwh_schema}.dim_countries);
insert into stage (select country_code, country_name from {stg_schema}.countries where code_status = 'VALID');
 
begin transaction;        
delete from {dwh_schema}.dim_countries t 
using stage 
where t.country_code = stage.country_code;

insert into {dwh_schema}.dim_countries 
select * from stage;

end transaction;
drop table stage; 
 
 """.format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_us_states = """ drop table if exists {dwh_schema}.dim_us_states; """.format(dwh_schema=edw_schema)
create_dim_us_states = """ create table if not exists {dwh_schema}.dim_us_states(state_code   varchar(10), 
                                                                                state    varchar(100),
                                                                                cities_count    integer,
                                                                                Female_Population   integer,
                                                                                Male_Population     integer,
                                                                                Total_Population    INTEGER,
                                                                                Median_Age          decimal(5,2)
                                                                                ); """.format(dwh_schema=edw_schema)

populate_dim_us_states = """ 
create temp table stage (like {dwh_schema}.dim_us_states);
insert into stage (
select a.state_code, coalesce(b.state, a.state_name) State, cities_count, Female_Population, Male_Population, Total_Population, b.Median_Age
                from {stg_schema}.us_states a
                left join (select "State Code" State_Code, "State" state, 
                                count(distinct "City") cities_count, 
                                sum(cast("Female Population" as integer)) Female_Population,
                                sum(cast("Male Population" as integer)) Male_Population,
                                sum(cast("Total Population" as integer)) Total_Population,
                                avg(cast("Median Age" as decimal(5,2))) Median_Age
                            from {stg_schema}.us_cities_demographics
                            group by 1, 2) b
                on trim(a.state_code) = trim(b.State_Code)
            );
            
begin transaction;        
delete from {dwh_schema}.dim_us_states t 
using stage 
where t.state_code = stage.state_code;

insert into {dwh_schema}.dim_us_states 
select * from stage;

end transaction;
drop table stage;
""".format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_visa_categories = """ drop table if exists {dwh_schema}.dim_visa_categories;""".format(dwh_schema=edw_schema)
create_dim_visa_categories = """ create table if not exists {dwh_schema}.dim_visa_categories(cat_code   integer, 
                                                                                                    cat_desc    varchar(100)
                                                                                                    ); """.format(dwh_schema=edw_schema)
populate_dim_visa_categories = """ 
create temp table stage (like {dwh_schema}.dim_visa_categories);
insert into stage (select cat_code, cat_desc from {stg_schema}.visa_categories);
 
begin transaction;        
delete from {dwh_schema}.dim_visa_categories t 
using stage 
where t.cat_code = stage.cat_code;

insert into {dwh_schema}.dim_visa_categories 
select * from stage;

end transaction;
drop table stage; 
""".format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_visa_types = """ drop table if exists {dwh_schema}.dim_visa_types; """.format(dwh_schema=edw_schema)
create_dim_visa_types = """ create table if not exists {dwh_schema}.dim_visa_types(type_code varchar(10), type_desc varchar(100));""".format(dwh_schema=edw_schema)
populate_dim_visa_types = """
 create temp table stage (like {dwh_schema}.dim_visa_types);
insert into stage (select distinct visatype from {stg_schema}.i94 where visatype is not null);

begin transaction;        
delete from {dwh_schema}.dim_visa_types t 
using stage 
where t.type_code = stage.type_code;

insert into {dwh_schema}.dim_visa_types 
select * from stage;

end transaction;
drop table stage; """.format(dwh_schema=edw_schema, stg_schema=stg_schema)


drop_dim_ports = """ drop table if exists {dwh_schema}.dim_ports; """.format(dwh_schema=edw_schema)
create_dim_ports = """ create table if not exists {dwh_schema}.dim_ports(port_code varchar(10), port_name varchar(100));""".format(dwh_schema=edw_schema)
populate_dim_ports = """
 create temp table stage (like {dwh_schema}.dim_ports);
insert into stage (select port_code, port_name from {stg_schema}.ports where Collapsed_code is null);
 
begin transaction;        
delete from {dwh_schema}.dim_ports t 
using stage 
where t.port_code = stage.port_code;

insert into {dwh_schema}.dim_ports 
select * from stage;

end transaction;
drop table stage; """.format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_port_modes = """ drop table if exists {dwh_schema}.dim_port_modes;""".format(dwh_schema=edw_schema)
create_dim_port_modes = """ create table if not exists {dwh_schema}.dim_port_modes(mode_code varchar(10), mode_desc varchar(100));""".format(dwh_schema=edw_schema)
populate_dim_port_modes = """
 create temp table stage (like {dwh_schema}.dim_port_modes);
insert into stage (select mode_code, mode_desc from {stg_schema}.port_modes);
 
begin transaction;        
delete from {dwh_schema}.dim_port_modes t 
using stage 
where t.mode_code = stage.mode_code;

insert into {dwh_schema}.dim_port_modes 
select * from stage;

end transaction;
drop table stage; """.format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_airlines = """ drop table if exists {dwh_schema}.dim_airlines;""".format(dwh_schema=edw_schema)
create_dim_airlines = """ create table if not exists {dwh_schema}.dim_airlines(airline_code varchar(10));""".format(dwh_schema=edw_schema)
populate_dim_airlines = """
create temp table stage (like {dwh_schema}.dim_airlines);
insert into stage (select distinct airline from {stg_schema}.i94 where airline is not null);
 
begin transaction;        
delete from {dwh_schema}.dim_airlines t 
using stage 
where t.airline_code = stage.airline_code;

insert into {dwh_schema}.dim_airlines 
select * from stage;

end transaction;
drop table stage;""".format(dwh_schema=edw_schema, stg_schema=stg_schema)

drop_dim_gender = """ drop table if exists {dwh_schema}.dim_gender;""".format(dwh_schema=edw_schema)
create_dim_gender = """ create table if not exists {dwh_schema}.dim_gender(gender_code varchar(10), gender_desc varchar(100));""".format(dwh_schema=edw_schema)
populate_dim_gender = """
create temp table stage (like {dwh_schema}.dim_gender);
insert into stage (select distinct gender_code,  gender_desc from {stg_schema}.gender);
 
begin transaction;        
delete from {dwh_schema}.dim_gender t 
using stage 
where t.gender_code = stage.gender_code;

insert into {dwh_schema}.dim_gender 
select * from stage;

end transaction;
drop table stage;""".format(dwh_schema=edw_schema, stg_schema=stg_schema)


##################################################################################################
drop_dwh_tables = [drop_dim_date, drop_dim_countries, drop_dim_us_states, drop_dim_visa_categories, drop_dim_visa_types,
                   drop_dim_ports, drop_dim_port_modes, drop_dim_airlines, drop_dim_gender]

create_dwh_tables = [create_dim_date, create_dim_countries, create_dim_us_states, create_dim_visa_categories, create_dim_visa_types,
                     create_dim_ports, create_dim_port_modes, create_dim_airlines, create_dim_gender]

populate_dwh_tables = [populate_dim_date, populate_dim_countries, populate_dim_us_states, populate_dim_visa_categories, populate_dim_visa_types,
                       populate_dim_ports, populate_dim_port_modes, populate_dim_airlines, populate_dim_gender]
