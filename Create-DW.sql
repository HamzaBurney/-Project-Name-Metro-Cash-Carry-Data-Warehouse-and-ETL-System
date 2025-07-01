DROP SCHEMA IF EXISTS METRO_DATAWAREHOUSE;
DROP SCHEMA IF EXISTS MASTER_DATA;

create schema METRO_DATAWAREHOUSE;
use METRO_DATAWAREHOUSE;

-- product table
CREATE TABLE product (
    product_id INT PRIMARY KEY NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

-- customer table
CREATE TABLE customer (
    customer_id INT PRIMARY KEY NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female'))
);

-- store table
CREATE TABLE store (
    store_id INT PRIMARY KEY NOT NULL UNIQUE,
    store_name VARCHAR(255) NOT NULL
);

-- supplier table
CREATE TABLE supplier (
    supplier_id INT PRIMARY KEY UNIQUE,
    supplier_name VARCHAR(255) NOT NULL
);

-- date table
CREATE TABLE date (
    time_id INT PRIMARY KEY NOT NULL UNIQUE,
    date_t DATE NOT NULL,
    time_t TIME NOT NULL,
    weekend BOOLEAN NOT NULL,
    half_of_year INT NOT NULL,
    month_t INT NOT NULL,
    quarter_t INT NOT NULL,
    year_t INT NOT NULL
);

-- sales table
CREATE TABLE sales (
    order_id INT PRIMARY KEY NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    supplier_id INT NOT NULL,
    time_id INT NOT NULL,
    store_id INT NOT NULL,
    quantity INT NOT NULL,
    sales_revenue DECIMAL(15, 2) NOT NULL
);

ALTER TABLE sales
ADD CONSTRAINT fk_product
FOREIGN KEY (product_id) REFERENCES product(product_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE sales
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE sales
ADD CONSTRAINT fk_supplier
FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE sales
ADD CONSTRAINT fk_time
FOREIGN KEY (time_id) REFERENCES date(time_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE sales
ADD CONSTRAINT fk_store
FOREIGN KEY (store_id) REFERENCES store(store_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;


CREATE SCHEMA MASTER_DATA;
USE MASTER_DATA;
-- customer table
CREATE TABLE customer (
    customer_id INT PRIMARY KEY NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female'))
);

-- product table
CREATE TABLE product (
    product_id INT PRIMARY KEY NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    store_id INT NOT NULL,
    store_name VARCHAR(255) NOT NULL
);

