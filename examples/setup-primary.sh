#!/bin/bash
set -e

# Create the replication user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER repuser WITH REPLICATION ENCRYPTED PASSWORD 'reppass';
EOSQL

# Create the orders table
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_orders_customer_name ON orders(customer_name);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
    CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);

    INSERT INTO orders (customer_name, amount, status) VALUES
        ('John Doe', 99.99, 'completed'),
        ('Jane Smith', 150.50, 'pending'),
        ('Bob Johnson', 75.25, 'completed')
    ON CONFLICT DO NOTHING;
EOSQL