
  
    

create or replace transient table GTM_DB.SILVER_GOLD.gold_lead_scores
    
    
    
    as (

WITH scored AS (
    SELECT
        LEAD_ID,
        SOURCE,
        FULL_NAME,
        EMAIL,
        EMAIL_DOMAIN,
        JOB_TITLE,
        SENIORITY,
        COMPANY_NAME,
        INDUSTRY,
        EMPLOYEE_COUNT,

        -- Seniority score (0-40)
        CASE
            WHEN SENIORITY IN ('c_suite','vp') THEN 40
            WHEN SENIORITY = 'director'         THEN 30
            WHEN SENIORITY = 'manager'          THEN 20
            WHEN SENIORITY = 'senior'           THEN 10
            ELSE 5
        END AS SENIORITY_SCORE,

        -- Company size score (0-30)
        CASE
            WHEN EMPLOYEE_COUNT >= 1000 THEN 30
            WHEN EMPLOYEE_COUNT >= 200  THEN 25
            WHEN EMPLOYEE_COUNT >= 50   THEN 15
            WHEN EMPLOYEE_COUNT >= 10   THEN 10
            ELSE 5
        END AS COMPANY_SIZE_SCORE,

        -- Industry fit score (0-20)
        CASE
            WHEN INDUSTRY IN (
                'software','technology','saas',
                'fintech','information technology'
            ) THEN 20
            WHEN INDUSTRY IN ('finance','banking','healthcare') THEN 15
            ELSE 5
        END AS INDUSTRY_SCORE,

        -- Completeness score (0-10)
        (CASE WHEN EMAIL        IS NOT NULL THEN 3 ELSE 0 END +
         CASE WHEN COMPANY_NAME IS NOT NULL THEN 2 ELSE 0 END +
         CASE WHEN JOB_TITLE    IS NOT NULL THEN 2 ELSE 0 END +
         CASE WHEN EMAIL_DOMAIN IS NOT NULL THEN 3 ELSE 0 END
        ) AS COMPLETENESS_SCORE

    FROM GTM_DB.SILVER_SILVER.silver_leads
),

final AS (
    SELECT *,
        SENIORITY_SCORE + COMPANY_SIZE_SCORE +
        INDUSTRY_SCORE  + COMPLETENESS_SCORE  AS LEAD_SCORE
    FROM scored
)

SELECT *,
    CASE
        WHEN LEAD_SCORE >= 80 THEN 'hot'
        WHEN LEAD_SCORE >= 60 THEN 'warm'
        WHEN LEAD_SCORE >= 40 THEN 'cool'
        ELSE 'cold'
    END AS INTENT_LEVEL,

    CASE
        WHEN LEAD_SCORE >= 80 THEN 'schedule_demo'
        WHEN LEAD_SCORE >= 60 THEN 'send_case_study'
        WHEN LEAD_SCORE >= 40 THEN 'add_to_nurture'
        ELSE 'enrich_data'
    END AS RECOMMENDED_ACTION,

    CURRENT_TIMESTAMP() AS SCORED_AT

FROM final
    )
;


  