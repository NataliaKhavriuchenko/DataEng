-- =====================================================================
-- TASK 3 — daily_activity (12 балів). Специфікація: ../../MODELS.md → «daily_activity».
-- Кількість подій по днях + накопичувальний підсумок: SUM(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    event_date,
    events,
    sum(events) over (
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_events
FROM (
    SELECT
        event_date,
        count(*) as events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
) as daily
ORDER BY event_date