CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL, name text, password_hash text, profile jsonb, active boolean DEFAULT true);
CREATE TABLE orders (id serial PRIMARY KEY, user_id int REFERENCES users(id), total numeric(10,2), notes text);
CREATE INDEX orders_user_idx ON orders(user_id);
INSERT INTO users (email, name, password_hash, profile) SELECT 'user' || g || '@example.com', 'User ' || g, 'hash' || g, jsonb_build_object('n', g) FROM generate_series(1, 300) g;
INSERT INTO orders (user_id, total, notes) VALUES (1, 10.50, 'card 4111 1111 1111 1111'), (2, 99.99, NULL);
