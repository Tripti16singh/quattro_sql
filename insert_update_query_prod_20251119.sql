--declare @sql NVARCHAR(max) = ''

--select @sql+= ' DROP TABLE ' + table_name + ';'
--FROM information_schema.tables
--where table_type= 'BASE TABLE'

--PRINT @sql

--EXEC sp_executesql @sql

--DROP TABLE dim_customer
--DROP TABLE dim_sow
--DROP TABLE dim_client
--DROP TABLE dim_vertical
--DROP TABLE dim_market
--DROP TABLE dim_employee
--DROP TABLE dim_sub_service
--DROP TABLE dim_service

--****************************************************0. DIM_CLIENT********************************************************************

-- drop table prod.dim_client

-- select * from prod.dim_client 
-- select * from stg.client_master

CREATE TABLE prod.dim_client (
    client_id INT IDENTITY(1,1) PRIMARY KEY,   
    consolidatedbp_code VARCHAR(50),  
    consolidatedbp_name VARCHAR(255), 
	start_date DATE,
	end_date DATE,
    created_by VARCHAR(100),  
    created_on DATETIME,  
    updated_by VARCHAR(100),  
    updated_on DATETIME  
);

--updating end date for client which is not present in the source anymore
UPDATE prod.dim_client 
set end_date = s.end_date, start_date = s.start_date, updated_by = 'Shashank', updated_on = GETDATE()
--select *
from (select distinct s.[consolidatedbp], s.[Consolidatedbp_name], end_date, start_date from stg.client_master s) s
INNER JOIN prod.dim_client d on ISNULL(d.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0) 
											and ISNULL(d.consolidatedbp_name, 0) = ISNULL(s.[Consolidatedbp_Name], 0)										
where COALESCE(d.end_date, '1900-01-01') <> COALESCE(s.end_date, '1900-01-01') 
		OR COALESCE(d.start_date, '1900-01-01') <> COALESCE(s.start_date, '1900-01-01')

--Inserting new data into dim_client which is present in the source but not in destination
INSERT INTO prod.dim_client (
    consolidatedbp_code, consolidatedbp_name, created_by, created_on, start_date, end_date
) 
SELECT s.[consolidatedbp], s.[Consolidatedbp_name], 'Shashank', getdate(), s.start_date, s.end_date
from (select distinct s.[consolidatedbp], s.[Consolidatedbp_name], start_date, end_date from stg.client_master s) s
LEFT JOIN prod.dim_client d on ISNULL(d.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0) 
											and ISNULL(d.consolidatedbp_name, 0) = ISNULL(s.[Consolidatedbp_Name], 0)
where d.consolidatedbp_code is null and d.consolidatedbp_name IS NULL
--GROUP BY s.[consolidatedbp], s.[Consolidatedbp_name]



--****************************************************1. DIM_CUSTOMER********************************************************************

-- drop table prod.dim_customer

-- select * from prod.dim_customer
-- select * from stg.client_master

--CREATE TABLE prod.dim_customer (
--    customer_id INT IDENTITY(1,1) PRIMARY KEY,  
--    customer_code VARCHAR(50) ,  
--    customer_name VARCHAR(255) ,
--	client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id),
--    group_name VARCHAR(100), 
--	client_industry NVARCHAR(100), 
--    alias_name VARCHAR(255),  
--    brand_name VARCHAR(255),  
--    pe_name VARCHAR(255),  
--	start_date DATE,
--	end_date DATE,
--    created_by VARCHAR(100),  
--    created_on DATETIME,  
--    updated_by VARCHAR(100),  
--    updated_on DATETIME  
--);

--updating existing records in dim_client which have changed as per client_code
UPDATE prod.dim_customer
set group_name = s.group_name
	, client_industry = s.[client_industry]
	, alias_name = s.[alias_name]
	, brand_name = s.[Brand]
	, pe_name = s.[pe_name]
	, start_date = s.start_date
	, end_Date = s.end_Date
	, updated_by = 'Shashank'
	, updated_on = getdate()
	--select *
from (select customer_code, customer_name, consolidatedbp, [Consolidatedbp_name], MAX(s.group_name) group_name
			, MAX(s.[client_industry]) [client_industry], MAX(s.[alias_name]) [alias_name]
			, MAX(s.[Brand]) [Brand], MAX(s.[pe_name]) [pe_name]
			, MAX(s.start_date) start_date, MAX(s.end_date) end_date
	  from stg.client_master s
	  group by customer_code, customer_name, consolidatedbp, [Consolidatedbp_name]) s
LEFT JOIN prod.dim_client c on ISNULL(c.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)
							and ISNULL(c.consolidatedbp_name, 0) = ISNULL(s.consolidatedbp_name, 0)
INNER JOIN prod.dim_customer d on ISNULL(d.customer_code, 0) = ISNULL(s.customer_code, 0) 
								and ISNULL(d.customer_name, 0) = ISNULL(s.customer_name, 0)
								and d.client_id = c.client_id
where	   COALESCE(d.group_name, '0') <> COALESCE(s.group_name , '0')
		OR COALESCE(d.client_industry, '0') <> COALESCE(s.[client_industry], '0')
		OR COALESCE(d.alias_name, '0') <> COALESCE(s.[alias_name]  , '0')
		OR COALESCE(d.brand_name, '0') <> COALESCE(s.[Brand]  , '0')
		OR COALESCE(d.pe_name, '0') <> COALESCE(s.[pe_name], '0')
		OR COALESCE(d.end_date , '1900-01-01')<> COALESCE(s.end_date, '1900-01-01') 
		OR COALESCE(d.start_date, '1900-01-01') <> COALESCE(s.start_date, '1900-01-01')

--updating end date for client which is not present in the source anymore
--UPDATE prod.dim_customer 
--set end_date = getdate(), updated_by = 'Shashank', updated_on = GETDATE()
--from stg.client_master s
--RIGHT JOIN prod.dim_client d on d.consolidatedbp_code = s.consolidatedbp and d.consolidatedbp_name = s.[Consolidateddb Name]										
--where s.consolidatedbp is NULL

--Inserting new data into dim_client which is present in the source but not in destination
INSERT INTO prod.dim_customer(
    customer_code, customer_name, client_id, group_name,
    client_industry, alias_name, brand_name, pe_name, created_by, created_on, start_date, end_date
) 
SELECT s.[Customer_Code]
      , s.[customer_name]
	  , c.client_id
	  , s.group_name
      , s.[client_industry]
      , s.[alias_name]
      , s.[Brand]
      , s.[pe_name]
      , 'Shashank'
	  ,  getdate()
	  ,  s.start_date
	  , s.end_date
from (select customer_code, customer_name, consolidatedbp, [Consolidatedbp_name], MAX(s.group_name) group_name
			, MAX(s.[client_industry]) [client_industry], MAX(s.[alias_name]) [alias_name]
			, MAX(s.[Brand]) [Brand], MAX(s.[pe_name]) [pe_name]
			, MAX(s.start_date) start_date, MAX(s.end_date) end_date
	  from stg.client_master s
	  group by customer_code, customer_name, consolidatedbp, [Consolidatedbp_name]) s
LEFT JOIN prod.dim_client c on ISNULL(c.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)
							and ISNULL(c.consolidatedbp_name, 0) = ISNULL(s.consolidatedbp_name, 0)
LEFT JOIN prod.dim_customer d on ISNULL(d.customer_code, 0) = ISNULL(s.customer_code, 0) 
								and ISNULL(d.customer_name, 0) = ISNULL(s.customer_name, 0)
								and d.client_id = c.client_id
where d.customer_name is null and d.customer_code IS NULL and d.client_id IS NULL


--****************************************************2. dim_market********************************************************************

--drop table prod.dim_market

--select * from prod.dim_market
--select * from stg.market_mapping

--CREATE TABLE prod.dim_market (
--    market_id INT IDENTITY(1,1) PRIMARY KEY,
--    market_name VARCHAR(255) NOT NULL,
--    start_date DATE NOT NULL,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

--updating end date for records which are no more in the source
--UPDATE prod.dim_market
--SET end_date = GETDATE(), updated_by = 'Shashank', updated_on = GETDATE()
--from (SELECT DISTINCT [market/BU] market from stg.[client_service_market_master]) s
--RIGHT join prod.dim_market d on  s.market = d.market_name 
--WHERE s.market IS NULL

--inserting new markets in dim_market which are not present in destination(dim_market) but are in the source[client_service_market_master]
INSERT INTO prod.dim_market (
    market_name, start_date, end_date, created_by, created_on
) 
SELECT DISTINCT s.market, GETDATE(), NULL,  'Shashank', GETDATE()
from (SELECT DISTINCT market from stg.vertical_mapping) s
left join prod.dim_market d on s.market = d.market_name
where d.market_name IS NULL

--select * from prod.dim_market

--update prod.dim_market set start_date = '2023-01-01'

--****************************************************3. DIM_vertical********************************************************************
--update prod.dim_vertical set start_date = '2023-01-01'

--drop table prod.dim_vertical 

--select * from prod.dim_vertical
--select * from stg.market_mapping

--CREATE TABLE prod.dim_vertical  (
--    vertical_id INT IDENTITY(1,1) PRIMARY KEY,
--    vertical_name VARCHAR(255) NOT NULL,
--    market_id int FOREIGN KEY REFERENCES prod.dim_market(market_id),
--    start_date DATE,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

--updating end date for records which are no more in the source
UPDATE prod.dim_vertical
SET  start_date = s.start_date, end_date = s.end_date, updated_by = 'Shashank', updated_on = GETDATE()
--select *
from (select DISTINCT vertical, market, start_date, end_date from stg.vertical_mapping) s
left join prod.dim_market dm on dm.market_name = s.market
inner join prod.dim_vertical d on s.vertical = d.vertical_name and dm.market_id = d.market_id
where COALESCE(s.end_date, '1900-01-01') <> COALESCE(d.end_date, '1900-01-01')
	or COALESCE(s.start_date, '1900-01-01') <> COALESCE(d.start_date, '1900-01-01')

--inserting new vertical and market combination in dim_vertical which are not present in destination(dim_vertical) but are in the source[client_service_market_master]
INSERT INTO prod.dim_vertical (
    vertical_name, market_id, start_date, end_date, created_by, created_on
) 
SELECT s.vertical, dm.market_id, s.start_date, s.end_Date,  'Shashank', GETDATE()
from (select DISTINCT vertical, market, start_date, end_date from stg.vertical_mapping) s
left join prod.dim_market dm on dm.market_name = s.market
left join prod.dim_vertical d on s.vertical = d.vertical_name and dm.market_id = d.market_id
where d.vertical_id IS NULL

--select * from prod.dim_vertical

--****************************************************4. DIM_SERVICE********************************************************************
--update prod.dim_service set start_date = '2023-01-01'
--drop table prod.dim_service

--select * from stg.service_mapping
--select * from prod.dim_service

--CREATE TABLE prod.dim_service (
--    service_id INT IDENTITY(1,1) PRIMARY KEY,
--    service_name VARCHAR(255) NOT NULL,
--    start_date DATE NOT NULL,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

--updating end date for records which are no more in the source
UPDATE prod.dim_service
SET end_date = s.end_date, updated_by = 'Shashank', updated_on = GETDATE()
from (SELECT DISTINCT Service, start_date, end_date from stg.service_mapping) s
inner join prod.dim_service d on s.Service = d.service_name

--inserting new services  which are not present in destination(dim_service) but are in the source[client_service_market_master]
INSERT INTO prod.dim_service (
    service_name, start_date, end_date, created_by, created_on
) 
SELECT DISTINCT s.Service, s.start_date, s.end_Date,  'Shashank', GETDATE()
from (SELECT DISTINCT Service, start_date, end_date from stg.service_mapping) s
left join prod.dim_service d on s.Service = d.service_name
where d.service_id IS NULL

--select * from prod.dim_service

--***********************************************************5. dim_sub_service************************************************************

--drop table prod.dim_sub_service

--select * from stg.service_mapping
--select * from prod.dim_sub_service

--CREATE TABLE prod.dim_sub_service (
--    sub_service_id INT IDENTITY(1,1) PRIMARY KEY,
--    sub_service_name VARCHAR(255) NOT NULL,
--    service_id INT FOREIGN KEY references prod.dim_service(service_id),
--    start_date DATE,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

--updating end date for records which are no more in the source
UPDATE prod.dim_sub_service
SET start_date = s.start_date, end_date = s.end_Date, updated_by = 'Shashank', updated_on = GETDATE()
from (SELECT DISTINCT Service, sub_service, start_Date, end_date from stg.service_mapping) s
LEFT JOIN prod.dim_service se on se.service_name = s.Service
inner join prod.dim_sub_service d on s.sub_service = d.sub_service_name and se.service_id = d.service_id
where COALESCE(s.end_date, '1900-01-01') <> COALESCE(d.end_date, '1900-01-01')
	or COALESCE(s.start_date, '1900-01-01') <> COALESCE(d.start_date, '1900-01-01')

INSERT INTO prod.dim_sub_service (
    sub_service_name, service_id, start_date, end_date, created_by, created_on
) 
SELECT s.sub_service, se.service_id, s.start_Date, s.end_date, 'Shashank', GETDATE()
from (SELECT DISTINCT Service, sub_service, start_Date, end_date from stg.service_mapping) s
LEFT JOIN prod.dim_service se on se.service_name = s.Service
left join prod.dim_sub_service d on s.sub_service = d.sub_service_name and se.service_id = d.service_id
where d.sub_service_id IS NULL

--select * from prod.dim_sub_service

--***********************************************************6. dim_department************************************************************

--drop table prod.dim_department

--select * from stg.department_allocation
--select * from prod.dim_department

--CREATE TABLE prod.dim_department (
--    department_id INT IDENTITY(1,1) PRIMARY KEY,
--    department_name VARCHAR(255) NOT NULL,
--    start_date DATE,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

--updating end date for records which are no more in the source
--UPDATE prod.dim_department
--SET end_date = GETDATE(), updated_by = 'Shashank', updated_on = GETDATE()
--from (SELECT DISTINCT department from stg.Trans_Department_Allocation) s
--left join prod.dim_department d on s.department = d.department_name
--where s.department IS NULL

--INSERT INTO prod.dim_department (
--    department_name, start_date, created_by, created_on
--) 
----SELECT s.department, GETDATE(),  'Shashank', GETDATE()
----from (SELECT DISTINCT department from stg.Trans_Department_Allocation) s
----left join prod.dim_department d on s.department = d.department_name
----where d.department_name IS NULL

--VALUES('Operations', GETDATE(),  'Shashank', GETDATE())
--, ('Operations Support', GETDATE(),  'Shashank', GETDATE())
--, ('IT Ops Support Team', GETDATE(),  'Shashank', GETDATE())
--, ('Sales', GETDATE(),  'Shashank', GETDATE())
--, ('G&A', GETDATE(),  'Shashank', GETDATE())

--select * from prod.dim_department



--select * from prod.dim_employee order by employee_name
--***********************************************************6. dim_item************************************************************

--drop table prod.dim_item

--select * from stg.item_master
--select * from prod.dim_item 

CREATE TABLE prod.dim_item (
    item_id INT IDENTITY(1,1) PRIMARY KEY,
    item_code VARCHAR(255),
    frequency VARCHAR(255),
	sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id),
    start_date DATE,
    end_date DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_by VARCHAR(100),
    updated_on DATETIME
);

--updating end date for records which are no more in the source
--UPDATE prod.dim_item
--SET end_date = GETDATE(), updated_by = 'Shashank', updated_on = GETDATE()
--from (select DISTINCT [Item Code] item_code, Frequency, [Practice/Service] practice, [Sub-service] sub_service from stg.item_master) s
--right join prod.dim_item d on s.item_code = d.item_code 
--where s.item_code IS NULL

--update mapping
UPDATE prod.dim_item 
SET sub_service_id = ss.sub_service_id, updated_by = 'Shashank', updated_on = GETDATE(), frequency = s.frequency
	, start_Date = s.start_Date, end_date = s.end_date
--select *
FROM prod.dim_item di
LEFT JOIN ( select * 
			from (
				select *, ROW_NUMBER()OVER(PARTITION BY item_code order by month_start desc) rn 
				from stg.item_master
			)a 
			where rn = 1
) s on s.item_code = di.item_code
INNER JOIN prod.dim_sub_service ss on ss.sub_service_name = s.sub_service
WHERE di.sub_service_id <> ss.sub_service_id or COALESCE(di.frequency, '0') <> COALESCE(s.frequency, '0')
		or COALESCE(di.start_date, '1900-01-01') <> COALESCE(s.start_date, '1900-01-01')
		or COALESCE(di.end_date, '1900-01-01') <> COALESCE(s.end_date, '1900-01-01')

INSERT INTO prod.dim_item (
    item_code, frequency, sub_service_id, start_date, end_date, created_by, created_on
) 
SELECT s.item_code, s.Frequency, ss.sub_service_id, s.start_date, s.end_date, 'Shashank', GETDATE()
from (select * from (select *, ROW_NUMBER()OVER(PARTITION BY item_code order by month_start desc) rn from stg.item_master)a where rn = 1) s
LEFT JOIN prod.dim_service se on se.service_name = s.service
LEFT JOIN prod.dim_sub_service ss on ss.sub_service_name = s.sub_service and ss.service_id = se.service_Id
left join prod.dim_item d on s.item_code = d.item_code 
where d.item_code IS NULL 
--GROUP BY s.item_code, s.Frequency, ss.sub_service_id

--********************************************************7.dim_employee************************************************************
--select * into prod.dim_employee_20251015 from prod.[dim_employee]
-- drop table prod.dim_employee

--select * from [stg].[employee_master] order by 1 
--select * from prod.[dim_employee] where department_id is null

CREATE TABLE prod.dim_employee (
    employee_id INT IDENTITY(1,1) PRIMARY KEY,
    employee_code VARCHAR(255),
    employee_name VARCHAR(255),
    designation VARCHAR(100),
    type VARCHAR(50),
    location VARCHAR(100),
    department_id INT FOREIGN KEY REFERENCES prod.dim_department(department_id),
    service_id INT FOREIGN KEY REFERENCES prod.dim_service(service_id),
	 reporting_manager_id INT,
    start_date DATE ,
    end_date DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME DEFAULT GETDATE(),
    updated_by VARCHAR(100),
    updated_on DATETIME DEFAULT GETDATE()    
);

----updating employees which have changed type and department in the source and destination based on employee code
update prod.dim_employee
set type=s.type, location=s.location, department_id=de.department_id, start_date = s.start_date, end_date = s.end_Date, updated_by =  'Shashank', updated_on = GETDATE()
--select *
from (select * from(select *, row_number()OVER(PARTITION BY employee_code, employee_name ORDER BY month_start)rn from stg.[employee_master])a where a.rn = 1 ) s
LEFT JOIN prod.dim_department de on de.department_name = s.Department
left join prod.dim_service se on s.[service] = se.service_name 
left join prod.dim_employee d on ISNULL(s.[employee_code],0) = ISNULL(d.employee_code , 0) 
where ISNULL(d.type, '0') <> ISNULL(s.type, '0') OR ISNULL(d.department_id, '0') <> ISNULL(de.department_id, '0') OR ISNULL(d.location, '0')<>ISNULL(s.location, '0') 
		OR ISNULL(d.start_date, '1900-01-01') <> ISNULL(s.start_date, '1900-01-01') OR ISNULL(d.end_date, '1900-01-01') <> ISNULL(s.end_Date, '1900-01-01')

--updating end date for employees which are not in the source table
--update prod.dim_employee
--set end_Date = GETDATE(), updated_by = 'Shashank', updated_on = GETDATE()
--from [stg].[zoho_employee_master] s
--right join prod.dim_service se on s.[Practice/Service] = se.service_name 
--left join prod.dim_employee d on ISNULL(s.[employee code],0) = ISNULL(d.employee_code , 0) and s.department = d.department and s.type = d.type
--													and s.Designation = d.designation and se.service_id = d.service_id
--where s.[employee code] is null

--inserting employee records in the table which are not present in the destination table
INSERT INTO prod.dim_employee (
    employee_code, employee_name, designation, type, location, department_id, 
    service_id, start_date, created_by, created_on
) 
select s.[employee_code], s.[employee_name], s.designation, s.type, s.location, de.department_id
		, se.service_id, s.start_date, 'Shashank', GETDATE()
from (select * from(select *, row_number()OVER(PARTITION BY employee_code, employee_name ORDER BY month_start)rn from stg.[employee_master])a where a.rn = 1 ) s
LEFT JOIN prod.dim_department de on de.department_name = s.Department
left join prod.dim_service se on s.[service] = se.service_name 
left join prod.dim_employee d on ISNULL(s.[employee_code],0) = ISNULL(d.employee_code , 0) 
where d.employee_id is null
--order by employee_code
--select * from dim_employee

--**************************************************************8.map_market_leader*******************************************************************
--drop table prod.map_market_leader

--select * from [stg].[vertical_mapping] 
--select * from prod.[map_market_leader] order by 2 

CREATE TABLE prod.map_market_leader (
    ml_mapping_id INT IDENTITY(1,1) PRIMARY KEY,
    market_id INT FOREIGN KEY REFERENCES prod.dim_market(market_id),
    market_leader_id NVARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_by VARCHAR(100),
    updated_on DATETIME
);

--updating end date where record no more exists in source
update prod.map_market_leader
set end_date = s.end_date, start_date = s.start_date, market_leader_id = e.employee_code,  updated_by = 'Shashank', updated_on = GETDATE()
--select *
from (select * from (Select *, ROW_NUMBER()OVER(PARTITION BY market, ml_leader ORDER BY month_start desc)rn from  stg.vertical_mapping)a where a.rn = 1) s
left join (select MAX(employee_code) employee_code, employee_name from prod.dim_employee group by employee_name) e on e.employee_name = s.ml_leader
left join prod.dim_market m on m.market_name = s.market
inner join prod.map_market_leader dest on ISNULL(dest.market_id,0) = ISNULL(m.market_id,0) 
where dest.market_leader_id <> e.employee_code or dest.start_date <> s.start_date or dest.end_date <> s.end_date

--updating mapping
--update prod.map_market_leader
--set market_leader_id = s.employee_id, updated_by = 'Shashank', updated_on = GETDATE()
--from prod.map_market_leader dest
--LEFT JOIN (
--			select m.market_id, e.employee_id
--			from (Select DISTINCT [market/BU] market, [market Leader Code] ml from  stg.[client_service_market_master]) s
--			left join prod.dim_employee e on e.employee_name = s.ml
--			left join dim_market m on m.market_name = s.market
--) s on ISNULL(dest.market_id,0) = ISNULL(s.market_id,0) 
--where dest.market_leader_id IS NULL and s.employee_id is not null

--inserting new records in the table which are not in the dev
INSERT INTO prod.map_market_leader
(    market_id, market_leader_id, start_date, end_date, created_by, created_on
) 
select m.market_id, e.employee_code, s.start_date, s.end_date, 'Shashank', GETDATE()
from (select * from (Select *, ROW_NUMBER()OVER(PARTITION BY market, ml_leader ORDER BY month_start desc)rn from  stg.vertical_mapping)a where a.rn = 1) s
left join (select MAX(employee_code) employee_code, employee_name from prod.dim_employee group by employee_name) e on e.employee_name = s.ml_leader
left join prod.dim_market m on m.market_name = s.market
left join prod.map_market_leader dest on ISNULL(dest.market_id,0) = ISNULL(m.market_id,0) 
										and ISNULL(dest.market_leader_id,0) = ISNULL(e.employee_code,0)
where dest.ml_mapping_id IS NULL

--****************************************************************************9. dim_sow****************************************************************
--drop table prod.dim_sow

--select * from  prod.dim_sow

--CREATE TABLE prod.dim_sow (
--    sow_id INT IDENTITY(1,1) PRIMARY KEY,
--    sow_code INT ,
--    client_id INT FOREIGN KEY REFERENCES dim_client(client_id) 
--	, sow_desc NVARCHAR(100)
--    , start_date DATE,
--    end_date DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME NOT NULL,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);

----updating end date for records which are no more in the source
--UPDATE prod.dim_sow
--SET end_date = GETDATE(), updated_on = GETDATE(), updated_by =  'Shashank'
--from stg.sow_master s
--INNER JOIN prod.dim_sow d on d.sow_code = s.[Sales Blanket Number]
--WHERE s.[Sales Blanket Number] IS NULL

--inserting new records which are not in destination
--INSERT INTO prod.dim_sow (
--    sow_code, client_id, sow_desc, start_date,  created_by, created_on
--) 
--SELECT s.[Sales Blanket Number], c.client_id, s.[SOW Desc], GETDATE(), 'Shashank', GETDATE() 
--from stg.sow_master s
--LEFT JOIN prod.dim_client c on c.consolidatedbp_code = s.consolidatedbp
--LEFT JOIN prod.dim_sow d on ISNULL(d.sow_code,0) = ISNULL(s.[Sales Blanket Number],0)
--WHERE d.sow_id IS NULL

--select * from  prod.dim_sow

--****************************************************************************10. map_sow****************************************************************
--drop table prod.map_sow

-- select * from prod.map_sow where vertical_id is null
-- select * from stg.client_Director_Mapping 



CREATE TABLE prod.map_sow (
  map_sow_id INT IDENTITY(1,1) PRIMARY KEY
  , sow_code NVARCHAR(100)
  ,	sow_desc NVARCHAR(255) 
  ,	client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id)
  --, vertical_id INT FOREIGN KEY REFERENCES prod.dim_vertical(vertical_id)
  , sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id)
  , start_date DATE
  , end_date DATE
  , created_by VARCHAR(100) NOT NULL
  , created_on DATETIME NOT NULL
  , updated_by VARCHAR(100)
  , updated_on DATETIME
);

--updating end date for records which are no more in the source
UPDATE prod.map_sow
SET end_date = s.end_Date, start_date = s.start_date, updated_on = GETDATE(), updated_by =  'Shashank'--, vertical_id = vertical.vertical_id
	, sub_service_id = ss.sub_service_id, client_id = cu.client_id, sow_desc = s.sow_description
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp)a
	where a.rn=1)s
LEFT JOIN prod.dim_vertical vertical on vertical.vertical_name = s.vertical 
LEFT JOIN prod.dim_sub_service ss on ss.sub_service_name = s.sub_service 
LEFT JOIN prod.dim_client cu on cu.consolidatedbp_code = s.consolidatedbp and cu.consolidatedbp_name = s.consolidateddb_name
INNER JOIN prod.map_sow dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0)
											and ISNULL(s.sow_code, 0) = ISNULL(dest.sow_Code, 0)
											and ISNULL(ss.sub_service_id, 0) = ISNULL(dest.sub_service_id, 0)
where ISNULL(dest.end_date, '1900-01-01') <> ISNULL(s.end_Date, '1900-01-01') or ISNULL(dest.start_date, '1900-01-01') <> ISNULL(s.start_date, '1900-01-01') --or ISNULL(dest.vertical_id, 0) <> ISNULL(vertical.vertical_id, 0) OR ISNULL(dest.sub_service_id,0) <> ISNULL(ss.sub_service_id,0) 
		OR ISNULL(dest.client_id, 0)  <> ISNULL(cu.client_id, 0) OR ISNULL(dest.sow_Desc,'') <> ISNULL(s.sow_description, '')


--inserting new records which are not in destination
INSERT INTO prod.map_sow (
    sow_code, sow_desc, client_id--, vertical_id
	, sub_service_id, start_date, end_date,  created_by, created_on
) 
SELECT  s.sow_code, s.sow_description, cu.client_id--, vertical.vertical_id
		, ss.sub_service_id	, s.start_date, s.end_date, 'Shashank', GETDATE() 
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp)a
	where a.rn=1)s
--LEFT JOIN prod.dim_vertical vertical on vertical.vertical_name = s.vertical 
LEFT JOIN prod.dim_sub_service ss on ss.sub_service_name = s.sub_service 
LEFT JOIN prod.dim_client cu on cu.consolidatedbp_code = s.consolidatedbp and cu.consolidatedbp_name = s.consolidateddb_name
LEFT JOIN prod.map_sow dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0)	and ISNULL(s.sow_code, 0) = ISNULL(dest.sow_Code, 0)
								and ISNULL(ss.sub_service_id, 0) = ISNULL(dest.sub_service_id, 0)
WHERE dest.map_sow_id IS NULL

--**************************************************************8. map_client_director*******************************************************************
--drop table prod.map_client_director

--	select * from stg.mapping 
--	select * from prod.map_client_director where map_sow_id = 1012
drop table if exists #temp
select * into #temp from stg.client_director_mapping

update #temp set sow_code = null where sow_code in ('', ' ','  ', '�', '��')
update #temp set sow_description = null where sow_description in ('', ' ','  ', '�', '��')
update #temp set sub_service = null where sub_service in ('', ' ','  ', '�', '��')

CREATE TABLE prod.map_client_director (
    cd_mapping_id INT IDENTITY(1,1) PRIMARY KEY
	  , sow_code NVARCHAR(100)
	  ,	sow_desc NVARCHAR(255) 
	  ,	client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id)
	  , sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id)
	, vertical_id INT,
    client_director_id NVARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_by VARCHAR(100) ,
    created_on DATETIME DEFAULT GETDATE(),
    updated_by VARCHAR(100),
    updated_on DATETIME DEFAULT GETDATE()
);

--updating end date where record no more exists in source
--update prod.map_client_director
--set end_date = GETDATE(), updated_by = 'Shashank', updated_on = GETDATE()
--from (Select DISTINCT consolidatedbp client_code, sow_code, client_director cd from  stg.mapping) s
--left join prod.dim_employee e on e.employee_code = s.cd
--left join prod.map_sow c on c.consolidatedbp_code = s.client_code and c.consolidatedbp_name = s.client_name
--right join prod.map_client_director dest on ISNULL(dest.client_id,0) = ISNULL(c.client_id,0) 
--															and ISNULL(dest.client_director_id,0) = ISNULL(e.employee_id,0)
--where s.client_code IS NULL

--updating mapping
update prod.map_client_director
set start_date = s.start_date, end_date = s.end_date, updated_by = 'Shashank', updated_on = GETDATE()
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp)a
	where a.rn=1)s
left join (select employee_name, MAX(employee_code) employee_code from prod.dim_employee group by employee_name) e on e.employee_name = s.client_director
left join prod.dim_vertical v on v.vertical_name = s.vertical
left join prod.dim_sub_service ss on ss.sub_service_name = s.sub_service
LEFT JOIN  prod.dim_client cu on ISNULL(cu.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)	and ISNULL(cu.consolidatedbp_name, 0) = ISNULL(s.consolidateddb_name, 0)									
inner join prod.map_client_director dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0) and ISNULL(dest.sub_service_id,0) = ISNULL(ss.sub_service_id,0)
										and ISNULL(dest.sow_code,0) = ISNULL(s.sow_code,0) and ISNULL(dest.client_director_id,0) = ISNULL(e.employee_code,0)
										and ISNULL(dest.vertical_id,0) = ISNULL(v.vertical_id,0)
where ISNULL(dest.end_date, '1900-01-01') <> ISNULL(s.end_Date, '1900-01-01') or ISNULL(dest.start_date, '1900-01-01') <> ISNULL(s.start_date, '1900-01-01') 


--inserting new records in the table which are not in the dev
INSERT INTO prod.map_client_director
(    sow_code, sow_desc, client_id, sub_service_id, vertical_id, client_director_id, start_date, end_date, created_by, created_on
) 
select s.sow_code, s.sow_description, cu.client_id, ss.sub_service_id, v.vertical_id, e.employee_code, s.start_date, s.end_date, 'Shashank', GETDATE()
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp)a
	where a.rn=1)s
left join (select employee_name, MAX(employee_code) employee_code from prod.dim_employee group by employee_name) e on e.employee_name = s.client_director
left join prod.dim_vertical v on v.vertical_name = s.vertical
left join prod.dim_sub_service ss on ss.sub_service_name = s.sub_service
LEFT JOIN  prod.dim_client cu on ISNULL(cu.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)	and ISNULL(cu.consolidatedbp_name, 0) = ISNULL(s.consolidateddb_name, 0)									
left join prod.map_client_director dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0) and ISNULL(dest.sub_service_id,0) = ISNULL(ss.sub_service_id,0)
										and ISNULL(dest.sow_code,0) = ISNULL(s.sow_code,0) and ISNULL(dest.client_director_id,0) = ISNULL(e.employee_code,0)
										and ISNULL(dest.vertical_id,0) = ISNULL(v.vertical_id,0)
where dest.cd_mapping_id IS NULL

-- select * from prod.map_client_director where vertical_id is null and client_director_id is null

--**************************************************************map_project_manager*******************************************************************
--drop table prod.map_project_manager

--	select * from stg.project_manager_mapping 
--	select * from prod.map_client_director 
drop table if exists #temp2
select * into #temp2 from stg.project_manager_mapping

update #temp2 set sow_code = null where sow_code in ('', ' ','  ', '�', '��')
update #temp2 set sow_description = null where sow_description in ('', ' ','  ', '�', '��')
update #temp2 set sub_service = null where sub_service in ('', ' ','  ', '�', '��')

CREATE TABLE prod.map_project_manager (
    pm_mapping_id INT IDENTITY(1,1) PRIMARY KEY
	, sow_code NVARCHAR(100)
	, sow_desc NVARCHAR(255) 
	, client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id)
	, sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id)
    , project_manager_id NVARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_by VARCHAR(100) ,
    created_on DATETIME DEFAULT GETDATE(),
    updated_by VARCHAR(100),
    updated_on DATETIME DEFAULT GETDATE()
);

--updating end date where record no more exists in source
--update prod.map_project_manager
--set end_date = GETDATE(),  updated_by = 'Shashank', updated_on = GETDATE()
--from (Select DISTINCT [SOW Code] sow,  [consolidatedbp] client_Code, [Sub-service] sub_service, [Practice/Service] practice, [Project Manager] pm from  stg.[client_service_market_master]) s
--left join dim_employee e on e.employee_name = s.pm
--left join dim_service se on se.service_name = s.practice
--left join dim_sub_service ss on ss.service_id = se.service_id and ss.sub_service_name = s.sub_service
--LEFT JOIN (
--			select s.sow_id, s.sow_code, sow_desc, c.consolidatedbp_code, c.consolidatedbp_name
--			from dim_sow s
--			LEFT JOIN dim_client c on s.client_id = c.client_id
--)d on ISNULL(d.sow_code,0) = ISNULL(s.sow,0) and d.consolidatedbp_code = s.client_code
--LEFT JOIN prod.map_sow ms on ms.sow_id = d.sow_id and ms.sub_service_id = ss.sub_service_id
--right join prod.map_project_manager dest on ISNULL(dest.map_sow_id,0) = ISNULL(ms.map_sow_id,0) 
--															and ISNULL(dest.project_manager_id,0) = ISNULL(e.employee_id,0)
--where ms.map_sow_id IS NULL and s.pm is null 

--update mapping
update prod.map_project_manager
set start_date = s.start_date, end_date = s.end_date, updated_by = 'Shashank', updated_on = GETDATE()
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp2)a
	where a.rn=1)s
left join (select employee_name, MAX(employee_code) employee_code from prod.dim_employee group by employee_name) e on e.employee_name = s.project_manager
left join prod.dim_sub_service ss on ss.sub_service_name = s.sub_service
LEFT JOIN  prod.dim_client cu on ISNULL(cu.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)	and ISNULL(cu.consolidatedbp_name, 0) = ISNULL(s.consolidateddb_name, 0)									
inner join prod.map_project_manager dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0) and ISNULL(dest.sub_service_id,0) = ISNULL(ss.sub_service_id,0)
										and ISNULL(dest.sow_code,0) = ISNULL(s.sow_code,0) and ISNULL(dest.project_manager_id,0) = ISNULL(e.employee_code,0)
where ISNULL(dest.end_date, '1900-01-01') <> ISNULL(s.end_Date, '1900-01-01') or ISNULL(dest.start_date, '1900-01-01') <> ISNULL(s.start_date, '1900-01-01') 

--inserting new records in the table which are not in the dev
INSERT INTO prod.map_project_manager (sow_code, sow_desc, client_id, sub_service_id, project_manager_id, start_date, end_date, created_by, created_on) 
select s.sow_code, s.sow_description, cu.client_id, ss.sub_service_id, e.employee_code, s.start_date, s.end_date, 'Shashank', GETDATE()
--select *
from(select * 
	from (select *, ROW_NUMBER()OVER(PARTITION BY consolidatedbp, consolidateddb_name, sow_code, sub_service ORDER BY month_start desc)rn from #temp2)a
	where a.rn=1)s
left join (select employee_name, MAX(employee_code) employee_code from prod.dim_employee group by employee_name) e on e.employee_name = s.project_manager
left join prod.dim_sub_service ss on ss.sub_service_name = s.sub_service
LEFT JOIN  prod.dim_client cu on ISNULL(cu.consolidatedbp_code, 0) = ISNULL(s.consolidatedbp, 0)	and ISNULL(cu.consolidatedbp_name, 0) = ISNULL(s.consolidateddb_name, 0)									
left join prod.map_project_manager dest on ISNULL(dest.client_id,0) = ISNULL(cu.client_id,0) and ISNULL(dest.sub_service_id,0) = ISNULL(ss.sub_service_id,0)
										and ISNULL(dest.sow_code,0) = ISNULL(s.sow_code,0) and ISNULL(dest.project_manager_id,0) = ISNULL(e.employee_code,0)
where dest.pm_mapping_id IS NULL --and employee_code IS NOT NULL

--select * from prod.map_project_manager

--**************************************************************fact_allocation*******************************************************************
-- drop table prod.fact_allocation
-- delete from prod.fact_allocation where month_start >= '2025-05-01'
--select * from stg.Department_Allocation order by employee_code
--select * from stg.vertical_Allocation order by employee_code
--select * from prod.fact_allocation

--update fact_allocation set month_start = '2024-09-01'

--CREATE TABLE prod.fact_allocation (
--    allocation_id INT IDENTITY(1,1) PRIMARY KEY,
--    employee_id INT FOREIGN KEY REFERENCES dim_employee(employee_id),
--	  department_id INT FOREIGN KEY REFERENCES dim_department(department_id),
--	  department_allocation DECIMAL(10,2),
--    vertical_id INT FOREIGN KEY REFERENCES dim_vertical(vertical_id),
--	  vertical_desc nvarchar(50),
--	  sub_service_id INT FOREIGN KEY REFERENCES dim_sub_service(sub_service_id),
--	  sub_service_desc nvarchar(50),
--	  allocation decimal(10,2),
--    month_start DATE,
--    created_by VARCHAR(100) NOT NULL,
--    created_on DATETIME NOT NULL,
--    updated_by VARCHAR(100),
--    updated_on DATETIME
--);


CREATE TABLE prod.fact_allocation (
    allocation_id INT IDENTITY(1,1) PRIMARY KEY,
    employee_id NVARCHAR(100),
	department_id INT,
	department_allocation float,
    vertical_id INT,
	vertical_desc nvarchar(50),
	sub_service_id INT,
	sub_service_desc nvarchar(50),
	allocation float,
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_by VARCHAR(100),
    updated_on DATETIME
);
--select * from dim_sub_service
INSERT INTO prod.fact_allocation(
	employee_id, department_id, department_allocation, vertical_id, vertical_desc, sub_service_id, sub_service_desc, allocation
	, month_start, created_by, created_on)
select s.employee_code, d.department_id, s.value, sv.vertical_id
		, case when s.vertical = 'Contingency' then s.vertical end vertical_desc
		, sse.sub_service_id
		, case when s.sub_service = 'Contingency' then s.sub_service end sub_service_desc
		, s.market_allocation, s.month, 'Shashank', GETDATE()
	--select *
from (
		select d.*, v.sub_services as sub_service, v.vertical, v.value as market_allocation
		from(
			SELECT 	employee_code, employee_name, department, value, DATEADD(day,1, EOMONTH(month, -1)) month
			FROM stg.Department_Allocation AS src
			UNPIVOT (value FOR department IN (
					operations,
					[operations support],
					[it ops support team],
					sales,
					[G&A]
				)) AS unpvt
			where value <> 0 --and employee_code like '%Jason Sangster%'
		) d
		LEFT JOIN (
				SELECT employee_code, employee_name, sub_services, vertical, value, DATEADD(day,1, EOMONTH(month, -1)) month
				FROM stg.vertical_Allocation AS src
				UNPIVOT (value FOR vertical IN (
						Auto,
						[Care Solutions],
						Retail,
						NFP,
						MME,
						[other MSP], channel, Contingency
					)) AS unpvt
				where value <> 0
				--order by 1
		) v on TRIM(v.employee_code) = TRIM(d.employee_code) and v.month = d.month
)s
--LEFT JOIN prod.dim_employee e on s.employee_code = e.employee_code
LEFT JOIN prod.dim_department d on d.department_name = s.department
LEFT JOIN prod.dim_vertical sv on sv.vertical_name = s.vertical 
LEFT JOIN prod.dim_sub_service sse on sse.sub_service_name = s.sub_service
WHERE not exists (  select 1 from prod.fact_allocation dest 
					where dest.month_start = s.month)
				--	order by 9

--select * from prod.fact_allocation				

--select distinct department_id from prod.fact_allocation a
--select distinct vertical_id from prod.fact_allocation a
--select distinct sub_service_id from prod.fact_allocation a

--checking invalid sub services in stg
--select distinct sub_services, sub_service_id from stg.vertical_Allocation d
--left join prod.dim_sub_service ss on ss.sub_service_name = d.sub_services
--where ss.sub_service_id is null

--**************************************************************fact_client_allocation*******************************************************************
-- drop table prod.fact_client_allocation

--select * from stg.Department_client_Allocation order by employee_code
--select * from stg.vertical_client_Allocation order by employee_code
--select * from prod.fact_client_allocation

--update fact_allocation set month_start = '2024-09-01'

CREATE TABLE prod.fact_client_allocation (
    allocation_id INT IDENTITY(1,1) PRIMARY KEY,
    employee_id NVARCHAR(100),
	department_id INT,
	department_allocation float,
    vertical_id INT,
	vertical_desc nvarchar(50),
	sub_service_id INT,
	sub_service_desc nvarchar(50),
	allocation float,
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_by VARCHAR(100),
    updated_on DATETIME
);
--select * from dim_sub_service
--select * from fact_client_allocation
INSERT INTO prod.fact_client_allocation(
	employee_id, department_id, department_allocation, vertical_id, vertical_desc, sub_service_id, sub_service_desc, allocation
	, month_start, created_by, created_on)
select distinct s.employee_code, d.department_id,  s.value, sv.vertical_id
		, case when s.vertical = 'Contingency' then s.vertical end vertical_desc
		, sse.sub_service_id
		, case when s.sub_service = 'Contingency' then s.sub_service end sub_service_desc
		, s.market_allocation, s.month, 'Shashank', GETDATE()
	--select *
from (
		select d.*, v.sub_service , v.vertical, v.value as market_allocation
		from(
			SELECT 	employee_code, employee_name, department_name as department, value, DATEADD(day,1, EOMONTH(month, -1)) month
			FROM stg.Department_client_Allocation AS src
			UNPIVOT (value FOR department_name IN (
					operations,
					[operations support],
					[it ops support team],
					sales,
					[G&A]
				)) AS unpvt
			where value <> 0
		) d
		LEFT JOIN (
				SELECT employee_code, employee_name, sub_services as sub_service, vertical, value, DATEADD(day,1, EOMONTH(month, -1)) month
				FROM stg.vertical_client_Allocation AS src
				UNPIVOT (value FOR vertical IN (
						Auto,
						[Care Solutions],
						Retail,
						NFP,
						MME,
						[Other MSP], Channel, Contingency
					)) AS unpvt
				where value <> 0
		) v on TRIM(v.employee_code) = TRIM(d.employee_code) and v.month = d.month
)s
--LEFT JOIN prod.dim_employee e on s.employee_code = e.employee_code
LEFT JOIN prod.dim_department d on d.department_name = s.department
LEFT JOIN prod.dim_vertical sv on sv.vertical_name = s.vertical 
LEFT JOIN prod.dim_sub_service sse on sse.sub_service_name = s.sub_service 
WHERE not exists (select 1 from prod.fact_client_allocation dest where dest.month_start = s.month)

--select * from prod.fact_client_allocation a
--***********************************************************fact_revenue************************************************

--drop table prod.fact_revenue

--select * from stg.revenue_flash group by inv_number
--select * from prod.fact_revenue where map_sow_id is null

CREATE TABLE prod.fact_revenue (
    revenue_id INT IDENTITY(1,1) PRIMARY KEY,
	org nvarchar(255),
    doc_type VARCHAR(50),
    inv_no nvarchar(255),
    doc_date date,
	customer_id int foreign key references prod.dim_customer(customer_id), 
	client_id int foreign key references prod.dim_client(client_id), 
    item_id int foreign key references prod.dim_item(item_id),
	sow_code NVARCHAR(100),
	sow_description NVARCHAR(500),
    description NVARCHAR(255),
	sal_unit_msr nvarchar(100),
	quantity float,
	rate float,
    line_total float,
    tax_rate float,
    total float,
	acct_code NVARCHAR(100),
    acct_name NVARCHAR(100),
    --map_sow_id INT FOREIGN KEY REFERENCES prod.map_sow(map_sow_id),
	--sub_service_id INT foreign key references dim_sub_service(sub_service_id), 
	industry nvarchar(100),
	sub_industry nvarchar(100),
	company_name nvarchar(255),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_revenue (
     org, doc_type, inv_no, doc_date, customer_id, client_id, sow_code, sow_description
	, description, sal_unit_msr, quantity, rate
	, line_total, tax_rate, total, acct_code, acct_name
    , item_id, industry, sub_industry, company_name
	, month_start, created_by, created_on
) 
SELECT s.org, s.doc_type, s.inv_number, s.doc_date, c.customer_id,cl.client_id, s.sow_code, s.sow_desc
	, s.description, s.sal_Unit_Msr, s.Quantity, s.Rate 
	, s.[Line_Total], s.TAX_RATE, s.TOTAL, s.acct_code, s.acct_name
	, i.item_id, s.business_unit , s.sbu ,s.company_name
	, DATEADD(day, 1, EOMONTH(s.doc_date,-1)), 'Shashank', GETDATE()
--select *
from stg.revenue_flash s
LEFT JOIN prod.dim_item i on i.item_code = s.[item_code]
LEFT JOIN prod.dim_client cl on ISNULL(cl.consolidatedbp_code,0) = ISNULL(s.consolidatedbp,0)
															and ISNULL(cl.consolidatedbp_name,0) = ISNULL(s.consolidatedbp_name,0)
LEFT JOIN prod.dim_customer c on ISNULL(c.customer_code,0) = ISNULL(s.customer_code,0) and ISNULL(c.customer_name,0) = ISNULL(s.customer_name,0)
															and c.client_id = cl.client_id
--where i.item_id is null
--LEFT join (	select sow_code, client_id,sub_service_id, MAX(map_sow_id) map_sow_id
--			from prod.map_sow
--			where sub_service_id IS NOT NULL
--			group by sow_code, client_id, sub_service_id
--) map_sow on ISNULL(map_sow.sow_code, 0) = ISNULL(s.sow_code, 0) and ISNULL(map_sow.client_id, 0) = ISNULL(c.client_id , 0)
--												and map_sow.sub_service_id = i.sub_service_id
--LEFT join (	select sow_code, client_id, MAX(map_sow_id) map_sow_id
--			from prod.map_sow
--			where sub_service_id is null
--			group by sow_code, client_id
--) map_sow2 on ISNULL(map_sow2.sow_code, 0) = ISNULL(s.sow_code, 0) and ISNULL(map_sow2.client_id, 0) = ISNULL(c.client_id , 0)
WHERE not exists (select 1 from prod.fact_revenue dest where EOMONTH(dest.doc_date,-1) = EOMONTH(s.doc_date,-1))

--select month_Start, sum(total) from prod.fact_revenue group by month_Start
--********************************************fact_US_payroll****************************************************************************

--drop table prod.fact_US_payroll

 --select * from stg.[fpo_us_payroll]
 --select * from stg.[cs_us_payroll]
 --select * from stg.[usw_us_payroll]

 --select * from prod.fact_US_payroll

CREATE TABLE prod.fact_US_payroll (
    us_payroll_id INT IDENTITY(1,1) PRIMARY KEY,
    payroll_month date,
    payrun_period NVARCHAR(100),
    check_date date,
    entity NVARCHAR(50),
	department nvarchar(50),
	employee_id NVARCHAR(100),
	working_state NVARCHAR(50),
	salary DECIMAL(18,2),
	total_salary DECIMAL(18,2),
	--salary NVARCHAR(max),
	--total_salary NVARCHAR(max),
	total_payroll_taxes DECIMAL(18,2),
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);
DECLARE @key VARBINARY(32) = 0x3BE21271B750529F25BF09204616A539A1AF5626D691BE9A22F86A1E67464DF66194072159D7C5002A6A41D7324D77A81B9F130E3D875D6A1EAB8141C2ECE7F4;

INSERT INTO  prod.fact_US_payroll (
    payroll_month, payrun_period, check_date, entity, break_up, employee_id, working_state
	, salary, total_salary, total_payroll_taxes
	, created_by, created_on
)
select payroll, [Payrun Period ], [Check Date], entity, [Break up], e.employee_id, [Working State]
		, salary, dbo.encryptSalary(total_salary, @key), total_payroll_taxes, s.created_by, s.created_on
from(
	select payroll, [Payrun Period ], [Check Date], entity, [Break up], [Employee Number], [Working State]
		, salary
		, dbo.DecryptSalary([Salary], @key) + [Regular] + [Overtime] + [PTO] + [Birthday PTO] + [Sick] + [Holiday] + [Bonus] + [Jury Duty] + 
			[Bereavement] + [Reimbursement-NT] + [Parking Reimbursement] + [Mass Transit Reimbursemen] + 
			[Retro Pay] + [Misc Earnings] + [Retro Hours] + [Volunteer] + [Per Dim] + [Volunteer PTO] as total_salary
		, [Fica (Employer)] + [Medicare (Employer)] + [FUTA] + [Paid Leave-DC (Employer)] + [SUI-FL] + [SUI-GA] + 
			[SUI-IL] + [SUI-IN] + [SUI-MA] + [SWT-MA] + [Massachusetts EMAC] + [MA-PFL (Employee)] + 
			[MA-PML (Employee)] + [SUI-MD] + [SUI-MI] + [SUI-MN] + [SUI-MO] + [SWT-MO] + [SUI-NC] + [SUI-NJ] +
			[SUI-NJ (Employee)] + [SWT-NJ] + [SDI-NJ] + [SDI-NJ (Employer)] + [Paid Leave-NJ (Employee)] + [SUI-NY] +
			[SUI-OH] + [SUI-OK] + [SWT-OK] + [SUI-OR] + [SWT-OR] + [WC-OR] + [WC-OR (Employer)] + 
			[OR EE Transit Tax] + [SUI-PA] + [SUI-TN] + [SUI-TX] + [SUI-WA] + [Paid Leave-WA (Employee)] + 
			[Paid Leave-WA (Employer)] + [SUI-WI] + [RD Fee] + [OH Canton] + [SUI-AZ] + [SUI-CA] + [SUI-UT] + 
			[SWT-UT] + [SUI-SC] + [SWT-SC] + [SUI-VA] + [SWT-VA] + [Medical Flat] + [Misc Net] + [401K Adj] + 
			[SUI-NH] + [PA 090805 Middletown Twp ] + [PA Middletown Twp LST (Bu] + [SUI-KY] + [SUI-LA] as total_payroll_taxes
		, 'Shashank' created_by, GETDATE() created_on
	from stg.[fpo_us_payroll]
	UNION ALL
	select payroll, [Payrun Period ], [Check Date], Entity, [Break up], [Employee Number], [Working State]
			, salary
			, dbo.DecryptSalary([Salary], @key)  + [Regular] + [Overtime] + [PTO] + [Birthday PTO] + [Sick] + [Holiday] + [Bonus]  + 
			[Jury Duty] + [Bereavement] + [Retro Pay] + [Misc Earnings] + [Bday] + [VLPTO] + [Retro Hours] as total_salary
			, [Medicare (Employer)] + [Fica (Employer)] + [FUTA] + [SUI-GA] + [SUI-MD] + [SUI-OH] + [SUI-WA] + 
				[Paid Leave-WA (Employer)] + [SUI-MN] + [SUI-IL] + [SUI-CA] + [SUI-MA] + [Massachusetts EMAC] + 
				[SUI-FL] + [SUI-NC] + [SUI-TN] + [SUI-TX] + [SUI-NJ] + [SDI-NJ (Employer)] as total_payroll_taxes
		, 'Shashank' created_by, GETDATE() created_on
	from stg.[cs_us_payroll]
	UNION ALL
	select Payroll, [Payrun Period ], [Check Date], Entity, [Sub Deprt], [Employee Number], [Working State]
		, Salary
		, dbo.DecryptSalary([Salary], @key)  + [Regular] + [Overtime] + [PTO] + [Birthday PTO] + [Sick] + [Holiday] + [Bonus] + 
			[Jury Duty] + [Bereavement] + [Retro Pay] + [Misc Earnings] + [Bday] + [ReimbN] + [VLPTO] + [Retro Hours] as total_salary
		, [Medicare (Employer)] + [Fica (Employer)] + [FUTA] + [Bankruptcy 1] + [Taxy Levy on Gross] + 
			[Trans- Illinois Only] + [SUI-NY] + [SUI-IL] + [SUI-MI] + [SUI-OK] + [SWT-OK] + [SUI-FL] + 
			[SUI-CA] + [SUI-CO] + [Colorado (employer)] + [SUI-NH] + [SUI-AR] + [SUI-GA] + [SUI-FL] as total_payroll_taxes
		, 'Shashank' created_by, GETDATE() created_on
	from stg.[usw_us_payroll]
)s
LEFT JOIN prod.dim_employee e on CAST(e.employee_code as VARCHAR) = CAST(s.[Employee Number] as VARCHAR)
where s.Payroll not in (select distinct payroll_month from prod.fact_US_payroll)


--********************************************fact_india_payroll****************************************************************************

--drop table prod.fact_india_payroll

 --select * from stg.[cs_india_payroll]
 --select * from stg.[qbss_india_payroll]

 --select * from prod.fact_india_payroll

CREATE TABLE prod.fact_india_payroll (
    india_payroll_id INT IDENTITY(1,1) PRIMARY KEY,
    payroll_month date,
	employee_id NVARCHAR(100),
	--working_location NVARCHAR(50),
	designation NVARCHAR(100),
	department NVARCHAR(50),
	--date_of_joining date,
	--date_of_leaving date,
	days_paid decimal(10,2), 
	basic_salary DECIMAL(18,2),
	calculated_gross_salary DECIMAL(18,2),
	--basic_salary NVARCHAR(max),
	--calculated_gross_salary NVARCHAR(max),
	employer_contribution_pf DECIMAL(18,2),
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_india_payroll (
    payroll_month, employee_id, designation, sap_mapping, days_paid, basic_salary, calculated_gross_salary,	employer_contribution_pf
	, created_by, created_on
)
SELECT s.Month, e.employee_id, s.Designation, s.[SAP Mapping] sap_mapping,  s.days_paid
		, dbo.DecryptSalary(s.[Basic Salary], @key) as [Basic Salary]
		, dbo.EncryptSalary(s.total_salary, @key) as total_salary
		, employer_contribution_pf
		, 'Shashank', GETDATE()			
from (
		select Month, [File Ref], [Employee Code], Designation, cast([Days Paid] as decimal(10,2)) days_paid, [SAP Mapping]
			, [Basic Salary]
			, dbo.DecryptSalary([Basic Salary], @key) + [House Rent Allownace] + [Bonus Statutory] + [Conveyance] + [LTA] + [Lunch_Allowance] + 
				[SODEX_COUPON] + [Tel Reimb] + [Other_Earnings] + [NSA_OCA] + [Holiday Pay] + [QPLB Annual] + 
				[Referral_Bonus] + [Performance_Bonus] + [Special_Allowance] as total_salary
			, [Employer Contribution PF] employer_contribution_pf
		from stg.[cs_india_payroll_encrypted]
		UNION
		select Month, [File Ref], [Employee Code], Designation, cast([Days Paid] as decimal(10,2)) days_paid, [SAP Mapping]
			, [Basic Salary]
			, dbo.DecryptSalary([Basic Salary], @key) + [HRA] + [Bonus Statutory] + [Misc Prof Allowance] + [LTA] + [Wellness Reimbursments] + 
				[Professional Development Reimbursement] + [Reimbursement of Telephone Exp] + [Extra Miler] + [Arrears Salary] + 
				[Holiday Pay] + [COE] + [Incentives] + [SPECIAL BONUS] + [Special Allowance] +
					CASE WHEN [File Ref] = 'Register' then [ESIC Payable]/0.75*3.25 else 0 end +
					CASE WHEN [File Ref] = 'Register' then [Labour Welfare Payable]*2 else 0 end +
					+ [Meal Allowance] + [QPLB Accural]
			, [Employer Contribution PF] employer_contribution_pf
		from stg.[qbss_india_payroll_encrypted]
)s
LEFT JOIN prod.dim_employee e on e.employee_code = s.[Employee Code] 
where s.month not in (select distinct payroll_month from prod.fact_india_payroll)


--********************************************fact_benefits****************************************************************************

--DROP TABLE prod.fact_benefits

--select * from stg.[us_benefits]
--select * from stg.[india_benefits]

--select * from prod.fact_benefits

CREATE TABLE prod.fact_benefits (
    benefit_id INT IDENTITY(1,1) PRIMARY KEY,
    benefit_name NVARCHAR(50),
	department_id INT FOREIGN KEY REFERENCES prod.dim_department(department_id),
    value decimal(18,10),
    location NVARCHAR(50),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_benefits(
	benefit_name, department_id, value, location, month_start, created_by, created_on
)
select s.particulars, d.department_id, s.value, s.location, DATEADD(day,1, EOMONTH(s.month, -1)) , 'Shashank', GETDATE()
from(
		SELECT 	particulars, department, value, month, 'US' as location
		FROM stg.us_benefits AS src
		UNPIVOT (value FOR department IN (
				operations,
				[operations support]
			)) AS unpvt
		where value <> 0
		UNION
		SELECT 	particulars, department, value, month, 'India' as location
		FROM stg.india_benefits AS src
		UNPIVOT (value FOR department IN (
				operations,
				[operations support]
			)) AS unpvt
		where value <> 0
)s
LEFT JOIN prod.dim_department d on d.department_name = s.department
where DATEADD(day,1, EOMONTH(s.month, -1)) not in (select DISTINCT month_start from prod.fact_benefits)


--********************************************fact_forex****************************************************************************
--drop table prod.fact_forex 

--select * from stg.forex_rate_input s
--select * from prod.fact_forex 

--select * from prod.fact_forex 

CREATE TABLE prod.fact_forex (
    forex_id INT IDENTITY(1,1) PRIMARY KEY,
    rate decimal(18, 10),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_forex(
	rate, month_start, created_by, created_on
)
select s.rate, DATEADD(day,1, EOMONTH(s.month, -1)), 'Shashank', GETDATE()
from stg.forex_rate_input s
where DATEADD(day,1, EOMONTH(s.month, -1)) not in (select DISTINCT month_start from prod.fact_forex)

--********************************************fact_contract_staff****************************************************************************

--alter table prod.fact_contract_staff  alter column vendors nvarchar(500)

 --drop table prod.fact_contract_staff 

 --select * from stg.[us_contract_staff]
 --select * from stg.[india_contract_staff]

 --select * from prod.fact_contract_staff 

CREATE TABLE prod.fact_contract_staff (
    contract_staff_id INT IDENTITY(1,1) PRIMARY KEY,
	entity NVARCHAR(500),
	vendors NVARCHAR(500),
    contractor_id NVARCHAR(100),
	amount decimal(18,10),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_contract_staff(
	entity, vendors, contractor_id, amount, month_start, created_by, created_on
)
select entity, vendors, s.contractor_code, s.amount, DATEADD(day, 1, EOMONTH(s.month, -1)), 'Shashank', GETDATE()
from (
	 select *, 'US' location  from stg.[us_contract_staff]
	 UNION ALL
	 select *, 'India' location from stg.[india_contract_staff]
 )s
--left join prod.dim_employee e on e.employee_code = s.[contractor_code]
where DATEADD(day,1, EOMONTH(s.month, -1)) not in (select DISTINCT month_start from prod.fact_contract_staff)

--********************************************dim_particulars****************************************************************************

--DROP TABLE prod.dim_particular

--select * from stg.particulars_mapping order by 2,3,4
--select * from prod.dim_particular order by 2,3,4

CREATE TABLE prod.dim_particular (
    particular_id INT IDENTITY(1,1) PRIMARY KEY,
	particular_name NVARCHAR(50), 
    pl_line_item NVARCHAR(50) ,
    sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id) ,
    start_date DATE,
    end_date DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_by VARCHAR(100),
    updated_on DATETIME
);

--update prod.dim_particular
--set sub_service_id = sse.sub_service_id
----select *
--from stg.particulars_mapping s
--LEFT JOIN prod.dim_sub_service sse on sse.sub_service_name = s.sub_service
--INNER JOIN prod.dim_particular d on d.particular_name  = s.particulars and d.pl_line_item = s.pl_line_item

INSERT INTO prod.dim_particular(particular_name, pl_line_item, sub_service_id, start_date, created_by, created_on)
select s.particulars,s.pl_line_item, sse.sub_service_id, DATEADD(day,1, EOMONTH(s.month_start, -1)), 'Shashank', GETDATE()
from stg.particulars_mapping s
LEFT JOIN prod.dim_sub_service sse on sse.sub_service_name = s.sub_service
LEFT JOIN prod.dim_particular d on d.particular_name  = s.particulars and d.pl_line_item = s.pl_line_item-- and EOMONTH(o.month,0) = EOMONTH(p.month_start,0)
WHERE d.particular_id IS NULL


--********************************************fact_other_costs****************************************************************************

--drop table prod.fact_other_costs

--select * from stg.[other_cost_input]
--select * from prod.fact_other_costs

--select v.vertical_name, ss.sub_service_Name, sum(amount) from prod.fact_other_costs a left join prod.dim_particular b on b.particular_id = a.particular_id
--left join prod.dim_vertical v on v.vertical_id = a.vertical_id
--left join prod.dim_sub_service ss on ss.sub_service_id = b.sub_service_id
--group by v.vertical_name, ss.sub_service_Name order by 1,2

--select vertical, sub_service, sum(value) from(
--SELECT  particulars, pl_line_item, month, vertical, value
--FROM stg.[other_cost_input] AS src
--UNPIVOT (value FOR vertical IN (
--		Auto,
--		[Care Solutions],
--		Retail,
--		NFP,
--		MME,
--		[Other MSP], channel, Contingency
--	)
--) AS unpvt
--where value <> 0)a
--left join stg.particulars_mapping b on b.particulars = a.particulars and b.pl_line_item = a.pl_line_item
--group by vertical, sub_service order by 1,2

CREATE TABLE prod.fact_other_costs (
    other_cost_id INT IDENTITY(1,1) PRIMARY KEY,
   -- other_cost_category NVARCHAR(50),
	particular_id INT FOREIGN KEY REFERENCES prod.dim_particular(particular_id),
	vertical_id INT FOREIGN KEY REFERENCES prod.dim_vertical(vertical_id),
	description nvarchar(50),
	amount decimal(18, 10),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_other_costs(
	 particular_id, vertical_id, description, amount, month_start, created_by, created_on
)
select  p.particular_id,  sv.vertical_id, CASE WHEN s.vertical = 'Contingency' then s.vertical end,  s.value
		, s.month_start,  'Shashank', GETDATE()
from (
	SELECT  particulars, pl_line_item, DATEADD(day,1, EOMONTH(month, -1)) month_start, vertical, value
	FROM stg.[other_cost_input] AS src
	UNPIVOT (value FOR vertical IN (
			Auto,
			[Care Solutions],
			Retail,
			NFP,
			MME,
			[Other MSP], channel, Contingency
		)
	) AS unpvt
	where value <> 0
) s
LEFT JOIN prod.dim_particular p on p.particular_name = s.particulars and p.pl_line_item = s.pl_line_item and s.month_start = p.start_date
LEFT JOIN prod.dim_vertical sv on sv.vertical_name = s.vertical
WHERE not EXISTS( SELECT 1 from prod.fact_other_costs where month_start = s.month_start)



--********************************************fact_subcontracting****************************************************************************
 
--drop table prod.fact_subcontracting

 --select * from stg.[subcontracting_input]
 --select * from prod.fact_subcontracting
 
 CREATE TABLE prod.fact_subcontracting (
    subcontracting_id INT IDENTITY(1,1) PRIMARY KEY,
	particular_id INT FOREIGN KEY REFERENCES prod.dim_particular(particular_id),
	client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id),
	sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id),
	sow_code NVARCHAR(100),
	amount decimal(18,10),
	remarks NVARCHAR(500),
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_subcontracting (
	particular_id, client_id, sub_service_id, sow_code, amount, remarks, month_start, created_by, created_on
)
SELECT p.particular_id, c.client_id, ss.sub_service_id, s.sow_code, s.Amount, s.Remarks, DATEADD(day,1, EOMONTH(s.month_start, -1)) as month_start, 'Shashank', GETDATE()
--select *
FROM stg.[subcontracting_input] s
LEFT JOIN prod.dim_particular p on p.particular_name = s.particulars and DATEADD(day,1, EOMONTH(s.month_start, -1)) = p.start_Date
left join prod.dim_client c on c.consolidatedbp_code = s.consolidatedbp_code and c.consolidatedbp_name = s.consolidatedbp_name
LEFT JOIN prod.dim_sub_service ss on s.[Sub_Service] = ss.sub_service_name
WHERE DATEADD(day,1, EOMONTH(s.month_start, -1)) not in (select DISTINCT month_start from prod.fact_subcontracting)

--********************************************fact_infra_costs****************************************************************************

--drop table prod.fact_infra_costs

 --select * from stg.[tech_infra_cost]
 --select * from stg.[tech_supp_vertical_allocation]
 --select * from stg.[tech_supp_subservice_allocation] 
 --select * from prod.[fact_infra_costs] order by 5 desc

CREATE TABLE prod.fact_infra_costs (
    infra_cost_id INT IDENTITY(1,1) PRIMARY KEY,
    cost_category NVARCHAR(50),
	attribute nvarchar(50),
	attribute_id INT,
	amount float,
    month_start DATE,
    created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME 
);

INSERT INTO prod.fact_infra_costs(
	 cost_category, attribute, attribute_id, amount, month_start, created_by, created_on
)
select  cost_category, s.attribute,  COALESCE(sv.vertical_id, ss.sub_service_id), s.amount, s.month,  'Shashank', GETDATE()
from (
	select 'Infra Cost' as cost_category, attribute as attribute, attribute as value,cost as amount, DATEADD(day,1, EOMONTH(month, -1)) as month
	from stg.[tech_infra_cost]

	UNION

	SELECT  'Infra Allocation', 'Vertical', vertical, value, DATEADD(day,1, EOMONTH(month, -1)) as month
	FROM stg.[tech_supp_vertical_allocation] AS src
	UNPIVOT (value FOR vertical IN (
		Auto,
			[Care Solutions],
			Retail,
			NFP,
			MME,
			[Other MSP], channel
		)
	) AS unpvt
	where value <> 0

	UNION
	SELECT  'Infra Allocation', 'Sub Service'
			, sub_service, value, DATEADD(day,1, EOMONTH(month, -1)) as month
	FROM stg.[tech_supp_subservice_allocation] AS src
	UNPIVOT (value FOR sub_service IN (
			[F&A],
			Payroll,
			Tax,
			[Reimbursable F&A],
			[Human Resources],
			[Reimbursable HR],
			[Talent Acquisition],
			[Application Support], MSP, [Hardware], Cloud, Cybersecurity, [Other msp], [Product Development & Automation]
		)
	) AS unpvt
	where value <> 0
) s
LEFT JOIN prod.dim_sub_service ss on ss.sub_service_name = s.value and s.attribute = 'Sub Service'
LEFT JOIN prod.dim_vertical sv on sv.vertical_name = s.value and s.attribute = 'Vertical'
WHERE not EXISTS( SELECT 1 from prod.fact_infra_costs where month_start = s.month)

--********************************************as_per_mis****************************************************************************
--drop table prod.as_per_mis
--select * from stg.as_per_mis
select * from prod.as_per_mis

CREATE TABLE prod.as_per_mis(
	mis_id INT IDENTITY(1,1) PRIMARY KEY
	, month_start date
	, report nvarchar(50)
	, item nvarchar(50)
	, cost float
	, created_by VARCHAR(100) NOT NULL,
    created_on DATETIME NOT NULL ,
    updated_by VARCHAR(100),
    updated_on DATETIME  
)

INSERT INTO prod.as_per_mis (month_start, report, item, cost, created_by, created_on)
select DATEADD(day,1, EOMONTH(month_start, -1)), report, item, cost, 'Shashank', GETDATE()
from stg.as_per_mis s
where not exists(select 1 from prod.as_per_mis where month_start = DATEADD(day,1, EOMONTH(s.month_start, -1)))

--********************************************fact_timesheet****************************************************************************
--drop table prod.fact_timesheet

--select * from prod.fact_timesheet where map_sow_id is null
--select * from stg.[timesheet]

CREATE TABLE prod.fact_timesheet (
			timesheet_id INT IDENTITY(1,1) PRIMARY KEY,
			client_id INT FOREIGN KEY REFERENCES prod.dim_client(client_id),
			sow_code nvarchar(100),
			employee_id NVARCHAR(100),
			sub_service_id INT FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id),
			hours decimal(18,10),
			month_start DATE,
			created_by VARCHAR(100) NOT NULL,
			created_on DATETIME NOT NULL ,
			updated_by VARCHAR(100),
			updated_on DATETIME 
);

INSERT INTO prod.fact_timesheet (
	employee_id, client_id, sow_code, sub_service_id, hours, month_start, created_by, created_on
)
SELECT s.employee_code, c.client_id, s.sow_code, ss.sub_service_id, s.Hours, DATEADD(day,1, EOMONTH(s.month_start, -1)) month_start, 'Shashank', GETDATE()
--select *
FROM stg.timesheet s
LEFT JOIN prod.dim_client c on c.consolidatedbp_code = s.client_code and c.consolidatedbp_name = s.client_name
--LEFT JOIN prod.dim_employee e on e.employee_code = s.[Employee_Code]        -- If both employee_code and Employee Code are numeric, compare them as BIGINT
LEFT JOIN prod.dim_sub_service ss on s.[Project_Description] = ss.sub_service_name
WHERE DATEADD(day,1, EOMONTH(s.month_start, -1)) not in (select DISTINCT month_start from prod.fact_timesheet) --and d.map_sow_id is null

--select * from stg.mapping where consolidatedbp in ('NFI00075')--, 'AA1002', 'APP00015', 'APP00040')

--select * from stg.timesheets_from_zoho
--select * from prod.fact_timesheet

--INSERT INTO dim_sub_service
--VALUES('Unmapped', 7,  '2025-04-01', null, 'Shashank', GETDATE(), null, null)

--INSERT INTO dim_service
--VALUES('Unmapped', '2025-04-01', null, 'Shashank', GETDATE(), null, null)

--INSERT INTO dim_vertical
--VALUES('Unmapped', 5,  '2025-04-01', null, 'Shashank', GETDATE(), null, null)

--INSERT INTO dim_market
--VALUES('Unmapped', '2025-04-01', null, 'Shashank', GETDATE(), null, null)

--INSERT INTO dim_client(consolidatedbp_code, consolidatedbp_name)
--VALUES('Unmapped', 'Unmapped')

--INSERT INTO dim_sow
--VALUES(null, 9, 'Unmapped', '2025-04-01', null, 'Shashank', '2025-04-01 13:16:23.120', null, null)

--INSERT INTO map_sow
--VALUES(17, 11, 8, '2025-04-01', null, 'Shashank', '2025-04-01 13:16:23.120', null, null)


--********************************************************************************************************************************

--drop table prod.[fact_budget]

CREATE TABLE prod.[fact_budget_new](
	[budget_id] [int] IDENTITY(1,1) NOT NULL,
	budget_mapping_id int null,
	[vertical_id] int FOREIGN KEY REFERENCES prod.dim_vertical(vertical_id),
	[sub_service_id] int FOREIGN KEY REFERENCES prod.dim_sub_service(sub_service_id),
	[Amount] [float] NULL,
	[month_start] [datetime] NULL,
	[created_by] [varchar](max) NULL,
	[created_on] [datetime] NULL,
	[updated_by] [varchar](max) NULL,
	[updated_on] [datetime] NULL,
)

INSERT INTO prod.[fact_budget_new]
select map.budget_mapping_id
		, v.vertical_id, ss.sub_service_id, CAST(b.amount as decimal(10,2))
		, DATEADD(day,1, EOMONTH(b.month_start, -1)) month_start, 'Shashank', GETDATE(), null, null 
from stg.budget_new b
left join prod.budget_mapping_new map on map.budget_category = b.category and ISNULL(map.budget_subcategory, 0) = ISNULL(b.subcategory, 0)
--left join prod.chart_of_accounts c on c.KPI_name = b.pl_item
LEFT JOIN prod.dim_vertical v on v.vertical_name = CASE WHEN b.vertical in ('Enterprise', 'Continueserve') then 'MME' else b.vertical end
LEFT JOIN prod.dim_sub_service ss on ss.sub_service_name = b.sub_service
where amount is not null and amount <> 0
--where map.budget_mapping_id is null or v.vertical_id is null or ss.sub_service_id is null

--select * from stg.budget_new
--select * from prod.fact_budget_new
--select * from prod.budget_mapping_new

--*****************************************************************************
drop table prod.fact_attrition
CREATE table prod.fact_attrition(
	id int primary key identity(1,1),
	client_id int foreign key references prod.dim_client(client_Id),
	vertical_id int foreign key references prod.dim_vertical(vertical_Id),
	last_billing_month date,
	fixed_revenue_loss_month float,
	created_by nvarchar(100),
	created_on date,
	updated_by nvarchar(100),
	updated_on date
)

INSERT INTO prod.fact_attrition(client_id,vertical_id,last_billing_month,fixed_revenue_loss_month,created_by,created_on,updated_by,updated_on)
select c.client_id, v.vertical_id, s.last_billing_month, s.fixed_revenue_loss_month, 'Shashank', GETDATE(), null, null
from [stg].[fact_attrition] s
left join prod.dim_client c on c.consolidatedbp_code = s.client_code and c.consolidatedbp_name = s.client_name
left join prod.vertical_category_mapping v on v.vertical_category = s.vertical_category

CREATE TABLE [dev].[fact_hist_attrition](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[client_id] [int] NULL,
	[vertical_id] [int] NULL,
	[last_billing_month] [date] NULL,
	[fixed_revenue_loss_month] [float] NULL,
	[in_year_revenue] [float] NULL,
	[acv_loss] [float] NULL,
	[prev_fy_revenue] [float] NULL,
	[current_fy_budget] [float] NULL,
	[current_fy_revenue] [float] NULL,
	[acv] [float] NULL,
	[attrition_month] [date] NULL,
	[fy] [nvarchar](100) NULL,
	[created_by] [nvarchar](100) NULL,
	[created_on] [date] NULL,
	[updated_by] [nvarchar](100) NULL,
	[updated_on] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

INSERT INTO prod.[fact_hist_attrition]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               (client_id,vertical_id,last_billing_month,fixed_revenue_loss_month,created_by,created_on,updated_by,updated_on)
select c.client_id, v.vertical_id, s.last_billing_month, s.fixed_revenue_loss_month
	, [in_year_revenue], [acv_loss], [prev_fy_revenue], [current_fy_budget], [current_fy_revenue]
	, [acv], [attrition_month], [fy], 'Shashank', GETDATE(), null, null
from [stg].[hist_attrition] s
left join prod.dim_client c on c.consolidatedbp_code = s.client_code and c.consolidatedbp_name = s.client_name
left join prod.vertical_category_mapping v on v.vertical_category = s.vertical_category

---- ---- added a new_deals _tables which was not present before to check the status of the version control.
CREATE TABLE dev.fact_newdeals (
    new_deal_id int IDENTITY(1,1) PRIMARY KEY,
    deal_id bigint,  
    client_id  INT FOREIGN KEY REFERENCES [dev].[dim_client](client_id), 
    --company_name NVARCHAR(255),
    deal_name VARCHAR(255) ,  
    bu_mapping_id int FOREIGN KEY REFERENCES [dev].[map_business_unit](bu_mapping_id),
    deal_owner VARCHAR(255),   
    qbss_services VARCHAR(255), 
    qbss_industry VARCHAR(255),  
    annual_contract_value DECIMAL(18, 2), 
    potential_acv_for_import DECIMAL(18, 2),  
    pipeline VARCHAR(255),  
    frequency_id int FOREIGN KEY REFERENCES [dev].[map_frequency](frequency_id),
    type_of_deal varchar(100),
    agreement_number varchar(100), 
    sow_id NVARCHAR(255) , 
    close_date DATE,  
    amount DECIMAL(18, 2),
    month varchar (100),
    created_by VARCHAR(100) NOT NULL,
	created_on DATETIME NOT NULL ,
	updated_by VARCHAR(100),
	updated_on DATETIME
) ;


INSERT INTO dev.fact_newdeals (
  deal_id, client_id, deal_name, bu_mapping_id, deal_owner, qbss_services, qbss_industry,
  annual_contract_value, potential_acv_for_import, pipeline, frequency_id, type_of_deal,
  agreement_number, sow_id, close_date, amount, [month], created_by, created_on
)
SELECT
  s.deal_id, c.client_id, s.deal_name, mb.bu_mapping_id, s.deal_owner, s.qbss_service, s.qbss_industry,
  TRY_CONVERT(decimal(18,2), s.annual_contract_value), TRY_CONVERT(decimal(18,2), s.potential_acv_for_import),
  s.pipeline, f.frequency_id, s.type_of_deal, s.agreement_number, TRY_CONVERT(int, s.sow_id),
  TRY_CONVERT(date, s.close_date), TRY_CONVERT(decimal(18,2), s.amount),
  LEFT(s.[month], 3),  -- keep as text: 'Jan','Feb',...
  'shashank', GETDATE()
FROM stg.fact_newdeals s
LEFT JOIN dev.dim_client        c  ON c.consolidatedbp_code = s.bp_code AND c.consolidatedbp_name = s.company_name
LEFT JOIN dev.map_business_unit mb ON mb.qbss_service_business_unit = s.qbss_servicing_business_unit
LEFT JOIN dev.map_frequency     f  ON f.frequency = s.frequency;
--select * from dev.fact_newdeals