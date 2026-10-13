CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) NOT NULL, name VARCHAR(255), balance DECIMAL(10,2), data BLOB, created DATETIME DEFAULT CURRENT_TIMESTAMP);
INSERT INTO users (email, name, balance, data) VALUES ('a@example.com', 'Alice', 12.50, X'DEADBEEF'), ('b@example.com', 'Bob', NULL, NULL);
