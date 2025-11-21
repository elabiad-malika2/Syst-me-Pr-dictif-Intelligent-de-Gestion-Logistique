CREATE DATABASE datadb;
-- create table (user may adapt)
CREATE TABLE IF NOT EXISTS predictions (
    event_time TEXT,
    order_region TEXT,
    order_city TEXT,
    sales DOUBLE PRECISION,
    order_item_quantity INTEGER,
    prediction DOUBLE PRECISION,
    probability TEXT
);
