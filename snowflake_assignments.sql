use role securityadmin;

-- 1. Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake. first commit
create role admin;
grant role admin to role sysadmin;

show roles;

create user admin password='SA123*8'
default_role = admin
must_change_password = True;

grant role admin to user admin;
grant role admin to role sysadmin;

show users;

-- 2. Create an M-sized warehouse named assignment_wh and use it for all the queries. 
use role accountadmin;
create warehouse assignment_wh with
warehouse_size='X-small'
auto_suspend=120
auto_resume=true;

grant usage on warehouse assignment_wh to role admin;
grant create database on account to role admin;


-- 3. Switch to the admin role. 
use role admin;


-- 4. Create a database assignment_db 
create database assignment_db;


-- 5. Create a schema my_schema
create schema assignment_db.my_schema;


-- 6. Create a table using any sample csv. You can get 1 by googling for sample csv’s. Preferably search for sample employee dataset so that you have PII related columns else you can consider any column as PII. 

create or replace storage integration my_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::577968817723:role/my_snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://aks-first-bucket/snowflake-assignments/')
   COMMENT = 'This an optional comment';

desc integration my_s3_int;

grant create stage on schema assignment_db.my_schema to role admin;
grant usage on integration my_s3_int to role admin;

create or replace stage my_s3_stage2 
    storage_integration = my_s3_int
    url='s3://aks-first-bucket/snowflake-assignments/';

show stages;
-- drop stage MY_S3_STAGE;
desc stage s3_stage;
list @my_s3_stage2;

Create table assignment_db.my_schema.employee(
    EMPLOYEE_ID number,
    FIRST_NAME varchar,
    LAST_NAME varchar,	
    EMAIL varchar,
    PHONE_NUMBER varchar,
    HIRE_DATE varchar,
    JOB_ID varchar,
    SALARY number,
    COMMISSION_PCT varchar,
    MANAGER_ID number,
    DEPARTMENT_ID number
);

copy into assignment_db.my_schema.employee
    from @my_s3_stage2
    file_format = (type=csv field_delimiter=',' skip_header=1)
    files = ('employees.csv')
    on_error = 'skip_file_5%';
    
select * from employee;    

create or replace file format my_json_unload_format
  type = 'JSON';
  
create or replace file format my_csv_unload_format
  type = 'CSV'
  field_delimiter = ',';
  
create or replace stage my_json_unload_stage 
    storage_integration = my_s3_int
    url='s3://aks-first-bucket/snowflake-assignments/'
    file_format = my_json_unload_format;  
    
-- 7. Also, create a variant version of this dataset.    
create table employee_variant (raw) as (
    SELECT OBJECT_CONSTRUCT(
        'EMPLOYEE_ID', EMPLOYEE_ID,
        'FIRST_NAME', FIRST_NAME,
        'LAST_NAME', LAST_NAME,	
        'EMAIL', EMAIL,
        'PHONE_NUMBER', PHONE_NUMBER,
        'HIRE_DATE', HIRE_DATE,
        'JOB_ID', JOB_ID,
        'SALARY', SALARY,
        'COMMISSION_PCT', COMMISSION_PCT,
        'MANAGER_ID', MANAGER_ID,
        'DEPARTMENT_ID', DEPARTMENT_ID
    ) FROM employee
);
    
select * from employee_variant;

select current_role();

-- 8. Load the file into an external and internal stage. 
-- unload the data into external stage
copy into @my_json_unload_stage/emp.json from employee_variant;

-- unload the data into internal stage
create or replace stage my_int_stage;

put file:///Users/abhaysingh/Downloads/employees.csv @my_int_stage;
-- getting error while running Unsupported feature.

-- 9. Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external. 

Create table assignment_db.my_schema.employee_ext(
    EMPLOYEE_ID number,
    FIRST_NAME varchar,
    LAST_NAME varchar,	
    EMAIL varchar,
    PHONE_NUMBER varchar,
    HIRE_DATE varchar,
    JOB_ID varchar,
    SALARY number,
    COMMISSION_PCT varchar,
    MANAGER_ID number,
    DEPARTMENT_ID number
);

copy into assignment_db.my_schema.employee_ext
    from @my_s3_stage2
    file_format = (type=csv field_delimiter=',' skip_header=1)
    files = ('employees.csv')
    on_error = 'skip_file_5%';
    

-- 10. Upload any unrelated parquet file to the stage location and infer the schema of the file.
-- parquet file format
create file format ff_parquet
    type = 'parquet';

create or replace stage my_parquet_stage
    storage_integration = my_s3_int
    url='s3://aks-first-bucket/snowflake-assignments/parquet_files/'
    file_format = ff_parquet;
    
list @my_parquet_stage;  

select * 
    from @my_parquet_stage
    (file_format => 'ff_parquet');

-- 11.Run a select query on the staged parquet file without loading it to a snowflake table.
-- query like structured tables
SELECT 
$1:__index_level_0__::int as index_level,
$1:cat_id::VARCHAR(50) as category,
DATE($1:date::int ) as Date,
$1:"dept_id"::VARCHAR(50) as Dept_ID,
$1:"id"::VARCHAR(50) as ID,
$1:"item_id"::VARCHAR(50) as Item_ID,
$1:"state_id"::VARCHAR(50) as State_ID,
$1:"store_id"::VARCHAR(50) as Store_ID,
$1:"value"::int as value
FROM @my_parquet_stage
(file_format => 'ff_parquet') limit 100;

--12. Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible.

-- set up roles
use role ACCOUNTADMIN;
create role analyst_masked;
create role analyst_full;

-- grant select on table to roles
grant select on table assignment_db.my_schema.employee to role analyst_masked;
grant select on table assignment_db.my_schema.employee to role analyst_full;

grant usage on database assignment_db to role analyst_masked;
grant usage on database assignment_db to role analyst_full;

grant usage on schema assignment_db.my_schema to role analyst_masked;
grant usage on schema assignment_db.my_schema to role analyst_full;

-- grant warehouse access to roles
grant usage on warehouse assignment_wh to role analyst_masked;
grant usage on warehouse assignment_wh to role analyst_full;


-- assign roles to a user
grant role analyst_masked to user abhaysnowflake;
grant role analyst_full to user abhaysnowflake;

-- create PII policy, phone_number, email
create or replace masking policy phone_number 
    as (val varchar) returns varchar ->
        case        
        when current_role() in ('ADMIN', 'ANALYST_FULL', 'ACCOUNTADMIN') then val
        else '##-###-##'
        end;
        
        
-- email policy
create or replace masking policy email 
    as (val varchar) returns varchar ->
        case        
        when current_role() in ('ADMIN', 'ANALYST_FULL', 'ACCOUNTADMIN') then val
        else '***************'
        end;
        
select current_role();

-- apply this policy on employee table
alter table if exists assignment_db.my_schema.employee modify column phone_number 
set masking policy phone_number;

alter table if exists assignment_db.my_schema.employee modify column email 
set masking policy email;

-- alter table if exists assignment_db.my_schema.employee modify column phone_number 
-- unset masking policy;

-- switch roles to see policy effect
use role analyst_masked;
select * from assignment_db.my_schema.employee;

use role analyst_full;
select * from assignment_db.my_schema.employee;



