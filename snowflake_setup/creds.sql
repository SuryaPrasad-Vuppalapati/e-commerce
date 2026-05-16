-- create accounts
use role accountadmin;

create warehouse if not exists ECOMMERCE_WH with warehouse_size='x-small';
create database if not exists ECOMMERCE_DB;
create role if not exists ECOMMERCE_ROLE;

show grants on warehouse ECOMMERCE_WH;

grant role ECOMMERCE_ROLE to user SuryaVuppalapati;
grant usage on warehouse ECOMMERCE_WH to role ECOMMERCE_ROLE;
grant all on database ECOMMERCE_DB to role ECOMMERCE_ROLE;

use role ECOMMERCE_ROLE;

create schema if not exists ECOMMERCE_DB.ECOMMERCE_SCHEMA;

-- clean up
use role accountadmin;

drop warehouse if exists ECOMMERCE_WH;
drop database if exists ECOMMERCE_DB;
drop role if exists ECOMMERCE_ROLE;
