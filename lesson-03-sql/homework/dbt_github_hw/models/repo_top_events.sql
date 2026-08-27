-- =====================================================================
-- TASK 2 — repo_top_events (12 балів). Специфікація: ../../MODELS.md → «repo_top_events».
-- TOP-5 репозиторіїв за кількістю подій у кожному event_type: ROW_NUMBER() + QUALIFY.
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    event_type,
    repo_name,
    count(*) AS event_count,
    ROW_NUMBER() OVER (
        PARTITION BY event_type
        ORDER BY COUNT(*) DESC, repo_name
    ) AS type_rank
FROM {{ ref('stg_events') }}
GROUP BY
    event_type,
    repo_name
QUALIFY type_rank <= 5


