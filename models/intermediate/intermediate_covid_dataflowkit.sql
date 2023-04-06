WITH dataflowkit

AS (SELECT  active_cases_text, 
            country_text, 
            last_update, 
            new_cases_text, 
            new_deaths_text, 
            total_cases_text, 
            total_deaths_text, 
            total_recovered_text
            
    FROM dw.stg_covid_dataflowkit t1
    WHERE CAST(t1.dt_load AS DATE) = (SELECT MAX(CAST(t2.dt_load AS DATE)) FROM dw.stg_covid_dataflowkit t2)
    )

SELECT *
FROM dataflowkit