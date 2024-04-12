WITH required_fields AS (
    SELECT
        CAST(id AS INTEGER) AS id,
        CAST(Name AS VARCHAR(255)) AS Name,
        CAST(TrainingName AS VARCHAR(255)) AS TrainingName,
        CAST(TrainerName AS VARCHAR(255)) AS TrainerName,
        CAST(Feedback AS VARCHAR(255)) AS Feedback
    FROM {{ source('etms', 'feedback')}}
)
SELECT * FROM required_fields
