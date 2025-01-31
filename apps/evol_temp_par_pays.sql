WITH

clean_data AS (
    SELECT
        dt, year(dt) as dt_year, month(dt) as dt_month,
        Country, City,
        CAST(AverageTemperature AS FLOAT) AS AverageTemperature
    FROM temp_by_city
    WHERE AverageTemperature IS NOT NULL
      --AND Country = 'France'
),

with_diffs AS (
    SELECT *,
        LAG(AverageTemperature) OVER(PARTITION BY Country, City, dt_month ORDER BY dt_year ASC) AS prev_year_temp,
        LAG(dt) OVER(PARTITION BY Country, City, dt_month ORDER BY dt_year ASC) as prev_dt,
        LAG(dt_year) OVER(PARTITION BY Country, City, dt_month ORDER BY dt_year ASC) as prev_year,
        (AverageTemperature - prev_year_temp) / (dt_year - prev_year) as temp_diff_by_year
    FROM clean_data
)

SELECT
    Country,
    City,
    -- round(temp_diff_by_year, 5) as temp_diff_by_year
    avg(temp_diff_by_year) AS avg_diff_temp_by_year
FROM with_diffs
WHERE temp_diff_by_year IS NOT NULL
GROUP BY Country, City
ORDER BY avg_diff_temp_by_year