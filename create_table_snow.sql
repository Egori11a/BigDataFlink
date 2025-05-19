-- Таблица: customers
CREATE TABLE IF NOT EXISTS customers (
    id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

-- Таблица: sellers
CREATE TABLE IF NOT EXISTS sellers (
    id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

-- Таблица: products
CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price DOUBLE PRECISION,
    quantity INTEGER,
    weight DOUBLE PRECISION,
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating DOUBLE PRECISION,
    reviews INTEGER,
    release_date TEXT,
    expiry_date TEXT
);

-- Таблица: stores
CREATE TABLE IF NOT EXISTS stores (
    name TEXT PRIMARY KEY,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

-- Таблица: suppliers
CREATE TABLE IF NOT EXISTS suppliers (
    name TEXT PRIMARY KEY,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

-- Таблица: sales (факт)
CREATE TABLE IF NOT EXISTS sales (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    seller_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    total_price DOUBLE PRECISION,
    sale_date TEXT,
    store_name TEXT,

    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (seller_id) REFERENCES sellers(id),
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (store_name) REFERENCES stores(name)
);