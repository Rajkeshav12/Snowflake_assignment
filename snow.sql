show roles;
 create role admin;
 grant role admin to role accountadmin;
 -- Creating the 'developer' role and granting role to admin
 create role developer;
 grant role developer to role admin;

  -- Creating the 'PII' role and granting role to accountadmin
 create role PII;
 grant role PII to role accountadmin;
 
-- creating datawarehouse assignment_wh
CREATE WAREHOUSE assignment_wh 
WITH WAREHOUSE_SIZE = 'MEDIUM'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

show warehouses;

-- granting privileges to admin role
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
use role admin;
-- creating database and schema
create or replace database assignment_db;
create or replace schema my_schema;

-- creating table which we will copy from external staging

CREATE OR REPLACE TABLE employee(
    first_name Number,
    last_name VARCHAR(255),
    email VARCHAR(100),
    phone VARCHAR(50),
    gender VARCHAR(10),
    department VARCHAR(255),
    job_title VARCHAR(255),
    years_of_experience Number,
    salary Number
);

-- creating table to copy from internal staging
CREATE OR REPLACE TABLE in_employee(
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(100),
    phone VARCHAR(50),
    gender VARCHAR(10),
    department VARCHAR(255),
    job_title VARCHAR(255),
    years_of_experience Number,
    salary Number
);
-- creating variant table 
create or replace table json_employee(
 json_raw_data variant
);

-- file format for json
create or replace file format json_format
type='json'
strip_outer_array=true;

-- staging for json file
create or replace stage json_stage file_format=json_format;
list @json_stage;

-- copying into json_employee table
copy into json_employee
from @json_stage/employee.json
file_format=json_format;

-- querying on json_employee
select * from json_employee;


show tables;

-- creating stage for internal_stage
create or replace stage internal_stage;
show stages;
list @internal_stage;

-- creating file format for csv type file
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;

-- copying into in_employee from internal stage
COPY INTO in_employee(
    first_name,
    last_name,
    email,
    phone,
    gender,
    department,
    job_title,
    years_of_experience,
    salary
    )
FROM
    (
        SELECT
            emp.$1,
            emp.$2,
            emp.$3,
            emp.$4,
            emp.$5,
            emp.$6,
            emp.$7,
            emp.$8,
            emp.$9
        FROM
            @internal_stage/employees.csv.gz (file_format => my_csv_format) emp
    );

select * from in_employee;


-- creating storage integeration from s3 bucket
CREATE STORAGE INTEGRATION s3_integration 
 type = external_stage 
 storage_provider ='s3'
 enabled = true 
 storage_aws_role_arn = 'arn:aws:iam::003905319674:role/snowflake_role' 
 storage_allowed_locations = ('s3://snowflakerb123/employee.csv');
 
-- As we are working on admin role, we grant all on integration object
GRANT ALL ON INTEGRATION s3_integration TO ROLE admin;
-- Describing Integration object to arrange a rrelatoinship between aws and snowflake
DESC INTEGRATION s3_integration;

-- doing external staging form storage external integration
CREATE OR REPLACE STAGE external_stage URL = 's3://snowflakerb123/employee.csv' STORAGE_INTEGRATION = s3_integration FILE_FORMAT = my_csv_format;

list @external_stage;

-- copying into table form external staging and querying from employee
COPY INTO employee(
        first_name,
        last_name,
        email,
        phone,
        gender,
        department,
        job_title,
        years_of_experience,
        salary
    )
FROM
    (
        SELECT
            emp.$1,
            emp.$2,
            emp.$3,
            emp.$4,
            emp.$5,
            emp.$6,
            emp.$7,
            emp.$8,
            emp.&9
        FROM
            @external_stage(file_format => my_csv_format) emp
    );

-- query on employee
select * from employee;



-- creating stage for parquet file
create or replace stage parquet_stage;

list @parquet_stage;

-- file format for parquet
CREATE FILE FORMAT my_parquet_format
  TYPE = parquet;

  -- infering the schema of parquet file
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage',
      FILE_FORMAT=>'my_parquet_format'
      )
    );

List @parquet_stage;

-- quering on parquet file form staging itself
SELECT * from @parquet_stage/employees.parquet;


-- creating masking policy on developer roleASSIGNMENT_DB

use role accountadmin;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE developer;
GRANT USAGE ON DATABASE "ASSIGNMENT_DB" to role "DEVELOPER";
GRANT USAGE ON SCHEMA "MY_SCHEMA" to role "DEVELOPER";
GRANT SELECT ON TABLE assignment_db.my_schema.in_employee to role "DEVELOPER";

-- GRANT apply MASKING POLICY on  hideEmail_mask TO ROLE developer;

-- creating masking policy for hiding email on developer
CREATE OR REPLACE MASKING POLICY hideEmail_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ADMIN', 'PII') THEN val
    ELSE '****'
  END;
  -- applying masking policy on column email
ALTER TABLE IF EXISTS in_employee MODIFY COLUMN email SET MASKING POLICY hideEmail_mask;

-- creating masking policy on phone for developer role
CREATE OR REPLACE MASKING POLICY hidePhone_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ADMIN', 'PII') THEN val
    ELSE '****'
  END;

  -- on column phone we are applying masking policy hidephone_mask
ALTER TABLE IF EXISTS in_employee MODIFY COLUMN phone SET MASKING POLICY hidePhone_mask;

-- changing the role to developer and then checking the masking applied or not
use role developer;
select * from in_employee;

use role accountadmin;
-- granting privileges and permission on database,datawarehouse and schmea on role PII
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE "ASSIGNMENT_DB" to role "PII";
GRANT USAGE ON SCHEMA "MY_SCHEMA" to role "PII";
GRANT SELECT ON TABLE assignment_db.my_schema.in_employee to role "PII";

-- changing the role to PII and querying the table
use role PII;
select * from in_employee;