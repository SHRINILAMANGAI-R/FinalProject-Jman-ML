WITH stg_users AS (
    SELECT
        *
    FROM {{ref('stg_users')}}
),

required_fields AS (
    SELECT
        tu.*,
        t.TrainingName,
        t.TrainingDate,
        TO_TIMESTAMP(t.TrainingStartTime::STRING, 'HH24:MI:SS') AS TrainingStartTime_ts,
        TO_TIMESTAMP(t.TrainingEndTime::STRING, 'HH24:MI:SS') AS TrainingEndTime_ts,
        t.TrainerName,
        t.ScheduledBy,
        t.ScheduledTo,
        t.t_id,
        t.ModuleCompletion
    FROM
        stg_users tu
    INNER JOIN
        {{ ref('stg_training') }} t ON tu.id = t.id
),

training_insights AS (
    SELECT
        ScheduledTo AS Role,
        TrainingName,
        -- Total number of training sessions
        COUNT(*) AS Total_Training_Sessions,
        -- Average duration of training sessions
        AVG(DATEDIFF(second, TrainingStartTime_ts, TrainingEndTime_ts)) AS avg_training_duration_seconds,
        -- Maximum duration of a training session
        MAX(DATEDIFF(second, TrainingStartTime_ts, TrainingEndTime_ts)) AS max_training_duration_seconds,
        -- Minimum duration of a training session
        MIN(DATEDIFF(second, TrainingStartTime_ts, TrainingEndTime_ts)) AS min_training_duration_seconds,
        -- Total number of completed modules
        Max(ModuleCompletion) AS Max_Completed_Modules
    FROM
        required_fields
    WHERE
        ScheduledTo IN ('Intern', 'Employee') -- Filter by ScheduledTo column
    GROUP BY
        ScheduledTo,
        TrainingName
)
SELECT
    Role,
    TrainingName,
    Total_Training_Sessions,
    -- Convert average training duration from seconds to hours format and round off to 2 decimal places
    ROUND(avg_training_duration_seconds / 3600, 2) AS avg_training_duration_hours,
    -- Convert maximum training duration from seconds to hours format and round off to 2 decimal places
    ROUND(max_training_duration_seconds / 3600, 2) AS max_training_duration_hours,
    -- Convert minimum training duration from seconds to hours format and round off to 2 decimal places
    ROUND(min_training_duration_seconds / 3600, 2) AS min_training_duration_hours,
    Max_Completed_Modules
    
FROM
    training_insights