BEGIN;

CREATE TABLE IF NOT EXISTS retail.customers (
	id integer NOT NULL,
	name character varying(255),
	username character varying(255),
	email character varying(255),
	lat double precision,
	lng double precision,
	temperature double precision,
	weather_condition character varying(255),
	city character varying(255),
	PRIMARY KEY(id)
);

COMMIT;



BEGIN;

CREATE TABLE IF NOT EXISTS retail.sales (
	order_id integer NOT NULL,
	product_id integer,
	customer_id integer NOT NULL,
	order_date date,
	price double precision,
	quantity integer,
	amount double precision
);

COMMIT;

ALTER TABLE IF EXISTS retail.sales
	ADD CONSTRAINT sales_customer_id_fkey
	FOREIGN KEY (customer_id)
	REFERENCES retail.customers (id);