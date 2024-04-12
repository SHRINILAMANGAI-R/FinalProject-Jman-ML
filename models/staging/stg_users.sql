WITH required_fields AS (
    SELECT
        id,
        CAST(Name AS VARCHAR(255)) AS Name,
        CAST(email AS VARCHAR(255)) AS email,
        CAST(username AS VARCHAR(100)) AS username,
        TO_DATE(dob, 'DD-MM-YYYY') AS dob,
        CAST(role AS VARCHAR(50)) AS role,
        CAST(department AS VARCHAR(100)) AS department
    FROM {{ source('etms', 'users')}}
)
SELECT * FROM required_fields
