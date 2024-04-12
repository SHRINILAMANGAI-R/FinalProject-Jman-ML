WITH required_fields AS (
    SELECT
        CAST(id AS INTEGER) AS id,
        CAST(TrainingName AS VARCHAR(255)) AS TrainingName,
        TO_DATE(TrainingDate, 'DD-MM-YYYY') AS TrainingDate, -- Assuming 'DD-MM-YYYY' is the current date format
        CAST(TrainingStartTime AS TIME) AS TrainingStartTime,
        CAST(TrainingEndTime AS TIME) AS TrainingEndTime,
        CAST(TrainerName AS VARCHAR(255)) AS TrainerName,
        CAST(ScheduledBy AS VARCHAR(255)) AS ScheduledBy,
        CAST(ScheduledTo AS VARCHAR(255)) AS ScheduledTo,
        CAST(t_id AS INTEGER) AS t_id,
        CAST(ModuleCompletion AS INTEGER) AS ModuleCompletion
    FROM {{ source('etms', 'training')}}
)
SELECT * FROM required_fields
