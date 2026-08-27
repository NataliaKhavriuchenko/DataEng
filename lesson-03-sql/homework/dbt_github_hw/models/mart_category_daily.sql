-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    e.event_date,
    d.is_weekend,
    c.category,
    count(e.event_type)  AS events,
    count(DISTINCT e.repo_name)  AS distinct_repos,
    count(DISTINCT e.actor_login)  AS distinct_actors
FROM {{ ref('stg_events') }} AS e
LEFT JOIN {{ ref('event_categories') }} AS c
    ON e.event_type = c.event_type
LEFT JOIN {{ ref('calendar') }} AS d
    on e.event_date = d.day 
GROUP BY e.event_date, d.is_weekend, c.category  
order by e.event_date

