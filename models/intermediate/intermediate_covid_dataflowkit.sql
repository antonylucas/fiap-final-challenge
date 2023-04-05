WITH dataflowkit
AS (SELECT *
    FROM dw.stg_covid_dataflowkit t1
    WHERE CAST(t1.dt_load AS DATE) = (SELECT MAX(CAST(t2.dt_load AS DATE)) FROM dw.stg_covid_dataflowkit t2)
    )

SELECT *
FROM dataflowkit