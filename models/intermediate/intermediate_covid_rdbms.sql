WITH rdbms
    AS (SELECT *
        FROM dw.stg_covid_rdbms a
        WHERE CAST(a.dt_load AS DATE) = (SELECT MAX(CAST(b.dt_load AS DATE))
                                         FROM dw.stg_covid_rdbms b)
        )



SELECT *
FROM rdbms



