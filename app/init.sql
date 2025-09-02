--Таблица delivery
CREATE TABLE delivery (
    delivery_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
	zip TEXT NOT NULL,
	city TEXT NOT NULL,
	address TEXT NOT NULL,
	region TEXT NOT NULL,
	email TEXT NOT NULL
);
--Таблица payment
CREATE TABLE payment (
    payment_id SERIAL PRIMARY KEY,
	transaction TEXT NOT NULL,
	request_id TEXT,
	currency VARCHAR(3),
	provider TEXT,
	amount INT,
	payment_dt INT,
	bank TEXT,
	delivery_cost INT,
	goods_total INT,
	custom_fee INT
);

CREATE TABLE items(
	items_id SERIAL PRIMARY KEY,
	chrt_id INT,
	track_number TEXT,
	price INT,
	rid TEXT,
	name TEXT,
	sale INT,
	size TEXT,
	total_price INT,
	nm_id INT,
	brand TEXT,
	status SMALLINT
);



-- Таблица orders с внешним ключом на  delivery, payment
CREATE TABLE orders (
    order_uid TEXT PRIMARY KEY,
	track_number TEXT,
	entry TEXT,
	delivery_id INTEGER UNIQUE NOT NULL REFERENCES delivery(delivery_id) ON DELETE CASCADE,
	payment_id INTEGER UNIQUE NOT NULL REFERENCES payment(payment_id) ON DELETE CASCADE,
	locale VARCHAR(2),
	internal_signature TEXT,
	customer_id TEXT,
	delivery_service TEXT,
	shardkey TEXT,
	sm_id INT,
	date_created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	oof_shard TEXT
);

CREATE TABLE orders_items(
	order_uid TEXT,
	item_id INT
)