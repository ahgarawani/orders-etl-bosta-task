-- Use the target database
USE sales_analytics_db;

-- 2. Create the Product Dimensions

-- a. Category Dimension
CREATE TABLE IF NOT EXISTS dim_category (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE
);

-- b. Tag Dimension
CREATE TABLE IF NOT EXISTS dim_tag (
    tag_id INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(255) NOT NULL UNIQUE
);

-- c. Product Table (with a foreign key to category)
CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_title VARCHAR(255) NOT NULL,
    product_description TEXT,
    product_price DECIMAL(10,2) NOT NULL,
    product_rating DECIMAL(3,2),
    product_stock INT,
    product_weight DECIMAL(10,2),
    product_height DECIMAL(10,2),
    product_width DECIMAL(10,2),
    product_depth DECIMAL(10,2),
    product_sku VARCHAR(100),
    category_id INT,
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
    INDEX idx_category_id (category_id)
);

-- d. Bridge Table for Product Tags (many-to-many relationship)
CREATE TABLE IF NOT EXISTS bridge_product_tag (
    product_id INT NOT NULL,
    tag_id INT NOT NULL,
    PRIMARY KEY (product_id, tag_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (tag_id) REFERENCES dim_tag(tag_id)
);

-- e. Product Review Dimension
CREATE TABLE IF NOT EXISTS dim_product_review (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    review_rating DECIMAL(3,2) CHECK (review_rating BETWEEN 0 AND 5),
    review_comment TEXT,
    review_date DATE,
    reviewer_name VARCHAR(255),
    reviewer_email VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    INDEX idx_product_id (product_id),
    INDEX idx_review_date (review_date)
);

-- 3. Create the Customer Dimensions

-- a. Customer Demographics Dimension
CREATE TABLE IF NOT EXISTS dim_customer_demo (
    demographics_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_birthdate DATE NOT NULL,
    customer_gender ENUM('Male', 'Female') NOT NULL, 
    customer_job VARCHAR(255)
);

-- b. Address Dimension 
CREATE TABLE IF NOT EXISTS dim_address (
    address_id INT AUTO_INCREMENT PRIMARY KEY,
    address_line VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    coordinates_lat DECIMAL(10,6),
    coordinates_lng DECIMAL(10,6),
    INDEX idx_city (city),
    INDEX idx_state (state),
    INDEX idx_country (country)
);

-- c. Main Customer Dimension linking to demographics and address
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    demographics_id INT,
    address_id INT,
    FOREIGN KEY (demographics_id) REFERENCES dim_customer_demo(demographics_id),
    FOREIGN KEY (address_id) REFERENCES dim_address(address_id),
    INDEX idx_demographics_id (demographics_id),
    INDEX idx_address_id (address_id)
);

-- 4. Fact Tables

-- a. Fact Sales table
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(100),
    product_id INT,
    customer_id INT,
    quantity INT,
    sales_total DECIMAL(10,2),
    discount_total DECIMAL(10,2),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    INDEX idx_product_id (product_id),
    INDEX idx_customer_id (customer_id)
);
