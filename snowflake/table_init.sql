

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

USE ROLE DBT_EXECUTOR_ROLE;


create or replace TABLE DATA_ENG_DBT.STAGING.ASSET_SOURCE (
	DATE DATE,
	OPEN NUMBER(38,14),
	HIGH NUMBER(38,14),
	LOW NUMBER(38,14),
	CLOSE NUMBER(38,14),
	ADJ_CLOSE NUMBER(38,14),
	VOLUME NUMBER(38,0),
	SYMBOL VARCHAR(16777216),
	CURRENCY VARCHAR(16777216),
	EXCHANGE VARCHAR(16777216),
	PREDICTION NUMBER(38,0),
	ACTUAL NUMBER(38,0),
	MODEL_ID VARCHAR(16777216)
);

CREATE STAGE my_stage;

PUT 'file://C:\\Users\\armym\\OneDrive\\New folder (2)\\Documents\\ds\\financialMLgh\\snowflake\\init_csv_tables\\asset_fact_table.csv' @my_stage;
PUT 'file://C:\\Users\\armym\\OneDrive\\New folder (2)\\Documents\\ds\\financialMLgh\\snowflake\\init_csv_tables\\position_fact_table.csv' @my_stage;


COPY INTO ASSET_SOURCE
  FROM @my_stage/asset_fact_table.csv
  FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' DATE_FORMAT = 'YYYY-MM-DD' SKIP_HEADER = 1)
  ON_ERROR = 'skip_file';


SELECT * FROM ASSET_SOURCE
ORDER BY DATE, SYMBOL
;


create or replace TABLE DATA_ENG_DBT.STAGING.POSITION_SOURCE (
	DATE DATE,
	TIME TIME(9),
	SYMBOL VARCHAR(16777216),
	CURRENCY VARCHAR(16777216),
	POSITION_SIZE NUMBER(38,5),
	TRADE_OPEN_SPREAD NUMBER(38,5),
	ACCOUNT_ID VARCHAR(16777216)
);


COPY INTO POSITION_SOURCE
  FROM @my_stage/position_fact_table.csv
  FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' DATE_FORMAT = 'YYYY-MM-DD' SKIP_HEADER = 1)
  ON_ERROR = 'skip_file';


SELECT * FROM PREDICTION_SOURCE;