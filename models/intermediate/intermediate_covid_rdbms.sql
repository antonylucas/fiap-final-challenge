WITH rdbms
    AS (SELECT  _airbyte_ab_id, 
                _airbyte_emitted_at, 
                continent, 
                country, 
                deaths_1m, 
                total_cases, 
                critical_condition, 
                total_deaths, 
                tests_1m, 
                total_cases_1m, 
                total_tests, 
                total_recovered, 
                active_cases, 
                population, 
                _airbyte_additional_properties 
        FROM dw.stg_covid_rdbms a
        WHERE CAST(a.dt_load AS DATE) = (SELECT MAX(CAST(b.dt_load AS DATE)) FROM dw.stg_covid_rdbms b)
        )


SELECT *
FROM rdbms



