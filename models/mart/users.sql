WITH RoleCounts AS (
    SELECT role as Role, COUNT(*) AS Count
    From {{ref('stg_users')}}
    WHERE department = 'Technical'
    GROUP BY role
)
SELECT Role, count FROM RoleCounts
