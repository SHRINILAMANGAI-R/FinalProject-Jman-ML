WITH stg_users AS (
    SELECT
        *
    FROM {{ref('stg_users')}}
),
stg_feedback AS (
    SELECT
        *
    FROM {{ref('stg_feedback')}}
),
feedback_analysis AS (
    SELECT
        TRAININGNAME,
        TRAINERNAME,
        SUM(CASE WHEN FEEDBACK = 'Excellent' THEN 1 ELSE 0 END) AS Excellent,
        SUM(CASE WHEN FEEDBACK = 'Very Good' THEN 1 ELSE 0 END) AS Very_Good,
        SUM(CASE WHEN FEEDBACK = 'Good' THEN 1 ELSE 0 END) AS Good,
        SUM(CASE WHEN FEEDBACK = 'Fair' THEN 1 ELSE 0 END) AS Fair,
        SUM(CASE WHEN FEEDBACK = 'Poor' THEN 1 ELSE 0 END) AS Poor
    FROM
        stg_feedback fb
    JOIN
        stg_users su ON fb.ID = su.ID
    WHERE
        su.role IN ('Intern', 'Employee') -- Filter by scheduled role
    GROUP BY
        TRAININGNAME,
        TRAINERNAME
)
SELECT
    TRAININGNAME,
    TRAINERNAME,
    Excellent,
    Very_Good,
    Good,
    Fair,
    Poor,
    ((Excellent * 5 + Very_Good * 4 + Good * 3 + Fair * 2 + Poor * 1) / NULLIF((Excellent + Very_Good + Good + Fair + Poor), 0)) * 20 AS Performance_Percentage
FROM
    feedback_analysis
ORDER BY
    TRAININGNAME,
    TRAINERNAME
