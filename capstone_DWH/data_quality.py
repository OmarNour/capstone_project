import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
stg_schema = config['STAGING_DB']['DB_SCHEMA_NAME']

invalid_arrival_or_departure_date = """update {stg_schema}.i94 set rejected = 1 where  arrival_date > departure_date; """.format(stg_schema=stg_schema)
invalid_cit_countries = """update {stg_schema}.i94 i set rejected = 1 
                            where  exists (select 1 from {stg_schema}.countries c 
                                                        where c.country_code = cast(i.i94cit as text) 
                                                        and c.code_status <> 'VALID' 
											); """.format(stg_schema=stg_schema)
invalid_res_countries = """update {stg_schema}.i94 i set rejected = 1 
                            where  exists (select 1 from {stg_schema}.countries c 
                                                        where c.country_code = cast(i.i94res as text) 
                                                        and c.code_status <> 'VALID' 
											); """.format(stg_schema=stg_schema)


run_data_qailty = [invalid_arrival_or_departure_date, invalid_cit_countries, invalid_res_countries]