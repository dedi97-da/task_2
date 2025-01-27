create schema stackoverflow_filtered;

set search_path to stackoverflow_filtered;


-- Raw Data For Customers Dataset
create table "customers" (
	"customer_id" text,
	"customer_unique_id" text,
	"zip_code" int4,
	"customer_city" text,
	"customer_state" text,
)
--Hash index on the customer_id column within the results table.
CREATE INDEX customer_id ON customers USING hash (customer_id);


-- Example Raw Data For Customers Dataset
create table "results" (
	"customer_id" text,
	"product_id" text,
	"product_category_name" int4,
	"customer_city" text,
	"customer_state" text,
)
--Hash index on the customer_id column within the results table.
CREATE INDEX customer_id ON customers USING hash (customer_id);

-- Noted : Here I just create sample of code for one table (customers) as a Raw data and results tables as 


