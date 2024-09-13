-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black; text-align:center; font-family:Arial;">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
SELECT
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points,
  RANK() OVER(
    ORDER BY
      AVG(calculated_points) DESC
  ) AS driver_rank
FROM
  f1_presentation.calculated_race_results
GROUP BY
  driver_name
HAVING
  COUNT(1) >= 50
ORDER BY
  avg_points DESC

-- COMMAND ----------

SELECT 
  CAST(CONCAT(CAST(race_year AS STRING), '-01-01') AS DATE) AS race_year_date,
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM
  f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY
  race_year, driver_name
ORDER BY
  race_year, avg_points DESC
