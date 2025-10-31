# data_warehouse

Project Overview : Create a datawarehouse for netflix streaming platform, using ETL (Extract, Transform and Loading).
Using Madellion architecture for refining and loading data from source systems to Target where data is utilized for analysis and reporting by different departments

Steps of Project :
Extract data from source systems in form of CRM and ERP files, CRM are Customer Relation Models where the tables contains data which is used independently in integration
The tables that contain user and content data which is not interdependent. ERP Enterprise Resource Planning, the tables which contains data that is interdependent of the 
user tables, it contains data that have business operations like inventory, financial, supply chain, content streaming, marketing activity and recommendations.

Follow Medallion Architecture :
- Create Bronze Layer : Loading data creating DDL and Stored Procedure.
                        Load Source Data from CRM and ERP tables to DDL tables. Create a stored procedure which truncates tables before inserting data into tables.
- Create Silver Layer : Data loading, transformation, cleaning, integration, enrichment, standardization and normalization.
                        Create the DDL for Silver layer. Check quality of Bronze layer. Transform and clean data using queries, load data into silver layer using insert query.
                        Create a stored Procedure that truncates and load data in Silver layer. Validate data correctness. Data Documenting and versioning in GIT.
- Create Gold Layer   : Creating view from the data into tables from silver layer. The tables are categorized into dimension and fact.
                        The view is derived from star schema that has a central fact table and dimension tables surrounding it. Fact tables
                        that contains the information representing facts like user subscription, reviews, recommendation and dimension tables
                        that have dimensions which contains data that have contextual information about user infromation , product information.
                        Explore Business Objects, Use star schema Make dimension and fact table views. Aggregate Objects that align with business objectives.
                        Check data for validating. Commit code to git.

Diagrams :  Data Architecture
            Data Integration
            Data Flow
            Data Mart
