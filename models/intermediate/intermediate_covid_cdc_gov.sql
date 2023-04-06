WITH cdc_gov
AS
(SELECT 'USA' AS country,
        a.*
FROM dev.dw.stg_covid_cdc_gov a

WHERE CAST(a.dt_load AS DATE) = (SELECT MAX(CAST(b.dt_load AS DATE)) FROM dev.dw.stg_covid_cdc_gov b))

SELECT *
FROM cdc_gov
