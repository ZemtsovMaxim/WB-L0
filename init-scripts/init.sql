CREATE DATABASE My_db;

-- Подключение к базе данных
\connect My_db;

CREATE USER My_user WITH PASSWORD '1234554321';
GRANT ALL PRIVILEGES ON DATABASE My_db TO My_user;
CREATE TABLE orders (
    order_uid VARCHAR PRIMARY KEY,
    order_data JSONB
);


