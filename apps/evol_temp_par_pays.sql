WITH

clean_data AS (
    SELECT
        dt,
        year(dt) AS dt_year,
        month(dt) AS dt_month,
        country,
        city,
        cast(averagetemperature AS float) AS avg_temperature
    FROM temp_by_city
    WHERE averagetemperature IS NOT null
    --AND Country = 'France'
),

with_diffs AS (
    SELECT
        country,
        city,
        lag(avg_temperature) OVER (w) AS prev_year_temp,
        year(lag(dt) OVER (w)) AS prev_year,
        (avg_temperature - prev_year_temp) / (dt_year - prev_year) AS temp_diff_by_year
    FROM clean_data
    WINDOW w AS (
        PARTITION BY country, city, dt_month
        ORDER BY dt_year ASC
    )
)

SELECT
    country,
    city,
    -- round(temp_diff_by_year, 5) as temp_diff_by_year
    avg(temp_diff_by_year) AS avg_diff_temp_by_year
FROM with_diffs
WHERE temp_diff_by_year IS NOT null
GROUP BY country, city
ORDER BY avg_diff_temp_by_year
