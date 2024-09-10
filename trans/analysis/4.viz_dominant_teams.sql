-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black; text-align:center; font-family:Arial;">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
SELECT
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points,
  RANK() OVER(
    ORDER BY
      AVG(calculated_points) DESC
  ) AS team_rank
FROM
  f1_presentation.calculated_race_results
GROUP BY
  team_name
HAVING
  COUNT(1) >= 100
ORDER BY
  avg_points DESC

-- COMMAND ----------

SELECT * FROM v_dominant_teams

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC   CAST(CONCAT(CAST(race_year AS STRING), '-01-01') AS DATE) AS race_year_date,
-- MAGIC   team_name,
-- MAGIC   COUNT(1) AS total_races,
-- MAGIC   SUM(calculated_points) AS total_points,
-- MAGIC   AVG(calculated_points) AS avg_points
-- MAGIC FROM
-- MAGIC   f1_presentation.calculated_race_results
-- MAGIC WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
-- MAGIC GROUP BY
-- MAGIC   race_year, team_name
-- MAGIC ORDER BY
-- MAGIC   race_year, avg_points DESC

-- COMMAND ----------

SELECT 
  CAST(CONCAT(CAST(race_year AS STRING), '-01-01') AS DATE) AS race_year_date,
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM
  f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY
  race_year, team_name
ORDER BY
  race_year, avg_points DESC
