
  
    

create or replace transient table GTM_DB.SILVER_SILVER.silver_leads
    
    
    
    as (

WITH cleaned AS (
    SELECT
        LEAD_ID,
        SOURCE,
        TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)          AS FULL_NAME,
        LOWER(TRIM(EMAIL))                                   AS EMAIL,
        SPLIT_PART(LOWER(TRIM(EMAIL)), '@', 2)               AS EMAIL_DOMAIN,
        LOWER(TRIM(JOB_TITLE))                               AS JOB_TITLE,
        LOWER(COALESCE(NULLIF(SENIORITY,'None'),'unknown'))  AS SENIORITY,
        TRIM(COMPANY_NAME)                                   AS COMPANY_NAME,
        LOWER(TRIM(COMPANY_DOMAIN))                          AS COMPANY_DOMAIN,
        LOWER(COALESCE(NULLIF(INDUSTRY,'None'),'unknown'))   AS INDUSTRY,
        EMPLOYEE_COUNT,
        APOLLO_SCORE,
        INGESTED_AT
    FROM GTM_DB.SILVER_BRONZE.bronze_leads
    WHERE EMAIL IS NOT NULL
      AND EMAIL != 'None'
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY EMAIL
            ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCLUDE (rn)
FROM deduped
WHERE rn = 1
    )
;


  