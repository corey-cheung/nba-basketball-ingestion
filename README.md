# nba-basketball-ingestion

**The ingestion part of an end-to-end data analytics pipeline using basketball data from the NBA.**

Repository for the ingestion of NBA basketball data from the public [Ball don't lie API.](https://www.balldontlie.io/home.html#introduction) This repo is focussed on data ingestion from a public API to a Postgres database using Python, and the ingestion of Postgres tables to DuckDB using Airbyte (probably). This simulates the real-life use case of ingesting data from an OLTP database to an OLAP database for analytics workloads. 

The resulting tables of this data ingestion will be used for data transformation using dbt in the `nba-basketball-dbt` repo. Further downstream, the transformed data will be used for analysis & visualisation in the `nba-basketball-analytics` repo.
