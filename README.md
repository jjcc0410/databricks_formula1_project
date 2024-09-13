# Databricks Formula 1 Project
---
## Description

This project leverages Databricks to analyze Formula 1 data. A dedicated Databricks workspace and an external Azure Storage account are used to ensure data persistence, even if the workspace is deleted. The external storage account is structured to support an efficient ETL process, with three main folders representing different stages of the data pipeline: `raw`, `processed`, and `presentation`.

### Storage Structure

1. **Raw Data**: Contains the original, unprocessed data.
   - **Historical Data**: Up to `2021/03/21`.
   - **Incremental Data**: Subfolders named `2021/03/28` and `2021/04/18` contain new data for incremental loading to avoid replacing existing data.

2. **Processed Data**: Stores data that has been cleaned and transformed for analysis. Located in the `trans` folder.

3. **Presentation Data**: Holds the final data and visualizations needed for reporting and insights. Located in the trans/analysis folder.

   Within the `trans/analysis` folder, there are two notebooks specifically designed to identify dominant drivers and dominant teams across a timeline. Additionally, there are two other notebooks dedicated to visualizing these findings, providing insights through dynamic and informative graphics.

### Data Pipeline Workflow

- **Ingestion**: Raw data that is externally managed is ingested and stored in the `processed` container.
- **Transformation**: data in the `processed` container goes through the `trans` notebook folder, where cleaning and transformations occur.
- **Visualization**: data in the `presentation` container goes through the `trans/analysis` folder to create visualizations for analysis.

This setup ensures data is handled efficiently, supports incremental data loading, and maintains the integrity and persistence of the dataset across different stages.

---

## Architecture
![imagen](https://github.com/user-attachments/assets/d1603f0f-8edf-4994-b7c4-3ba6dfb7384c)
<br/><br/><br/>
## Visualizations
![imagen](https://github.com/user-attachments/assets/580c8ac5-5be1-44a2-ab3f-108d15d379c2)
<br/><br/><br/>
![imagen](https://github.com/user-attachments/assets/0f4cd228-1e10-4daa-8b0f-2d842e228ff7)
