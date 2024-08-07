-- Create a database
CREATE or replace DATABASE PEI_project; 

-- Create a warehouse
CREATE OR REPLACE WAREHOUSE PEI_WH
 WAREHOUSE_TYPE = STANDARD
 WAREHOUSE_SIZE =  XSMALL 
  AUTO_RESUME =  TRUE
INITIALLY_SUSPENDED =  TRUE; 


-- CREATE A role that has access to both the database and warehouse
CREATE OR REPLACE ROLE PEI_ROLE; 

-- CREATE our tables
CREATE or replace TABLE Customers
  ( 
     Customer_ID INT PRIMARY KEY, 
     First    VARCHAR(100), 
     Last    VARCHAR(100), 
     Age INT,
     Country VARCHAR(100) 
  ); 

CREATE or replace TABLE Orders
  ( 
     Order_ID INT PRIMARY KEY, 
     Item   VARCHAR(100), 
     Amount    INT, 
     Customer_ID INT, 
     FOREIGN KEY (Customer_id) REFERENCES Customers(Customer_id)
  );  

  
CREATE or replace TABLE Shipping
  ( 
     Shipping_ID INT PRIMARY KEY, 
     Status  VARCHAR(100), 
     Customer_ID INT, 
     FOREIGN KEY (Customer_id) REFERENCES Customers(Customer_id)
  );

CREATE OR REPLACE TABLE shipping_data (
    data VARIANT
);


-- CREATE THE STORAGE INTEGRATION OBJECT

CREATE OR REPLACE STORAGE INTEGRATION pei_s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 's3'
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::047506089313:role/PEI_S3_Snowflake'
STORAGE_ALLOWED_LOCATIONS = ('s3://peiproject-sampy/customers/', 's3://peiproject-sampy/orders/','s3://peiproject-sampy/shipping/'); 
  

DESC INTEGRATION pei_s3_int; 


-- Let's load the data 

-- But first let's create a file format for the orders dataset 
CREATE OR REPLACE FILE format orders_file_format 
type = csv 
field_delimiter = ','
skip_header = 1;

--Create stage for the orders file
CREATE OR REPLACE STAGE orders_stage 
url = 's3://peiproject-sampy/orders/'
storage_integration = pei_s3_int
file_format = orders_file_format;

-- let's preview the data 
// select $1, $2, $3, $4 from @orders_stage; 

-- let's load the staged  data into the orders  table
COPY INTO orders
from @orders_stage; 

select * from orders; 


-- let's try to load the shipping json file into the shipping table in snowflake

CREATE OR REPLACE FILE format shipping_file_format 
type = json
strip_outer_array = true; 

-- create a stage for the shipping json data 

CREATE OR REPLACE STAGE shipping_stage 
url = 's3://peiproject-sampy/shipping/'
storage_integration = pei_s3_int
file_format = shipping_file_format;


-- then selectively copy the contents of the stage into the snowflake shipping table 
COPY INTO shipping from (
select $1:Shipping_ID::int as Shipping_ID,
    $1:Status::STRING as Status,
    $1:Customer_ID::int as Customer_ID 
    from @shipping_stage); 

-- select * from shipping; 

-- let's work on the orders table 


-- File format for the customer dataset 
-- Note: Even though the original customer dataset is in excel on aws s3, a lamdba function converts that 
-- excel file to csv  so that Snowflake can easily pull it in. 
-- Snowflake cannot or has a hard time pulling in excel files. 

CREATE OR REPLACE FILE FORMAT Customer_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Create the stage for the customer dataset 
CREATE OR REPLACE STAGE customer_stage 
url = 's3://peiproject-sampy/customers/'
storage_integration = pei_s3_int
file_format = Customer_file_format;

-- let's preview the customer stage dataset 
select $1, $2, $3, $4, $5 from @customer_stage;


-- lets load the staged data into the customer table on snowflake 
COPY INTO customers from (
 select $1, $2, $3, $4, $5 from @customer_stage
); 

-- let's do some testing of the datasets 

select count(*) from @customer_stage;

SELECT * FROM customers limit 10; 

select o.Order_id, o.item, o.Customer_ID, c.First, c.Last, c.Country 
from Orders as o
left join Customers c on o.customer_id = c.customer_id
limit 10; 

select s.Shipping_ID, s.Status, c.First, c.Last, c.Country 
from Shipping as s
left join Customers c on s.customer_id = c.customer_id
limit 10; 



-- let's create pipes to automatically ingest new data in the 3 stages into the respective tables

-- Customer snowpipe 
CREATE OR REPLACE pipe customer_pipe
auto_ingest = True
AS 
COPY INTO customers from (
 select $1, $2, $3, $4, $5 from @customer_stage
); 

DESC pipe customer_pipe; 


-- Orders snowpipe 
CREATE OR REPLACE pipe order_pipe
auto_ingest = True
AS 
COPY INTO orders
from @orders_stage;  

DESC pipe order_pipe; 


-- Shipping snowpipe 

CREATE OR REPLACE pipe shipping_pipe
auto_ingest = True
AS 
COPY INTO shipping from (
select $1:Shipping_ID::int as Shipping_ID,
    $1:Status::STRING as Status,
    $1:Customer_ID::int as Customer_ID 
    from @shipping_stage);  

DESC pipe shipping_pipe; 


