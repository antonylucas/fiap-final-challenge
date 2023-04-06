{{ config(
    materialized = "table"
) }}

WITH 
dataflowkit AS (SELECT * FROM {{ ref("intermediate_covid_dataflowkit") }}),
rdbms AS (SELECT * FROM {{ ref("intermediate_covid_rdbms") }}),
cdc_gov AS (
SELECT
          cdc.country as country_cdc,
          SUM(census2019) as census2019,
          SUM(doses_distributed) as doses_distributed,
          SUM(doses_administered) as doses_administered,
          SUM(dist_per_100k) as dist_per_100k,
          SUM(admin_per_100k) as admin_per_100k,
          SUM(administered_moderna) as administered_moderna,
          SUM(administered_pfizer) as administered_pfizer,
          SUM(administered_janssen) as administered_janssen,
          SUM(administered_unk_manuf) as administered_unk_manuf,
          SUM(administered_dose1_recip) as administered_dose1_recip,
          AVG(administered_dose1_pop_pct) as administered_dose1_pop_pct,
          AVG(administered_dose2_pop_pct) as administered_dose2_pop_pct,
          SUM(administered_dose1_recip_18plus) as administered_dose1_recip_18plus,
          AVG(administered_dose1_recip_18pluspop_pct) as administered_dose1_recip_18pluspop_pct,
          SUM(administered_18plus) as administered_18plus,
          SUM(admin_per_100k_18plus) as admin_per_100k_18plus,
          SUM(distributed_per_100k_18plus) as distributed_per_100k_18plus,
          SUM(administered_dose1_recip_65plus) as administered_dose1_recip_65plus,
          AVG(administered_dose1_recip_65pluspop_pct) as administered_dose1_recip_65pluspop_pct,
          SUM(administered_65plus) as administered_65plus,
          SUM(admin_per_100k_65plus) as admin_per_100k_65plus,
          SUM(distributed_per_100k_65plus) as distributed_per_100k_65plus,
          SUM(administered_dose2_recip) as administered_dose2_recip,
          SUM(administered_dose2_recip_18plus) as administered_dose2_recip_18plus,
          SUM(administered_dose2_recip_18pluspop_pct) as administered_dose2_recip_18pluspop_pct,
          SUM(distributed_moderna) as distributed_moderna,
          SUM(distributed_pfizer) as distributed_pfizer,
          SUM(distributed_janssen) as distributed_janssen,
          SUM(distributed_unk_manuf) as distributed_unk_manuf,
          SUM(series_complete_yes) as series_complete_yes,
          AVG(series_complete_pop_pct) as series_complete_pop_pct,
          SUM(series_complete_18plus) as series_complete_18plus,
          AVG(series_complete_18pluspop_pct) as series_complete_18pluspop_pct,
          SUM(administered_65plus_entity) as administered_65plus_entity,
          SUM(census2019_12pluspop) as census2019_12pluspop,
          SUM(administered_12plus) as administered_12plus,
          SUM(admin_per_100k_12plus) as admin_per_100k_12plus,
          SUM(distributed_per_100k_12plus) as distributed_per_100k_12plus,
          COUNT(longname) AS quantity_state


FROM (SELECT  *,
              'USA' AS country_cdc
        FROM  {{ ref("intermediate_covid_cdc_gov") }} 
        WHERE longname <> 'Puerto Rico') AS cdc


GROUP BY  cdc.country)

SELECT *
FROM dataflowkit a
INNER JOIN rdbms b  ON b.country     = a.country_text 
LEFT JOIN cdc_gov c ON c.country_cdc = a.country_text