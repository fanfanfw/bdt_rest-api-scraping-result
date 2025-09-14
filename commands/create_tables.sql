-- PostgreSQL DDL Script for Car Market Price Tables
-- Generated based on Django models (without created_at, updated_at, category_id)
-- Date: 2025-01-11

-- =============================================
-- 1. CARS_STANDARD TABLE (Reference table)
-- =============================================

CREATE TABLE IF NOT EXISTS cars_standard (
    id BIGINT PRIMARY KEY,
    
    -- Normalized car data fields
    brand_norm VARCHAR(100) NOT NULL,
    model_group_norm VARCHAR(100) NOT NULL,
    model_norm VARCHAR(100) NOT NULL,
    variant_norm VARCHAR(100) NOT NULL,
    
    -- Raw data fields (optional)
    model_group_raw VARCHAR(100) NULL,
    model_raw VARCHAR(100) NULL,
    variant_raw VARCHAR(100) NULL,
    variant_raw2 VARCHAR(100) NULL
);

-- Create indexes for cars_standard table
CREATE INDEX IF NOT EXISTS idx_cars_standard_brand_model_variant ON cars_standard (brand_norm, model_norm, variant_norm);
CREATE INDEX IF NOT EXISTS idx_cars_standard_brand ON cars_standard (brand_norm);
CREATE INDEX IF NOT EXISTS idx_cars_standard_model ON cars_standard (model_norm);

-- Add comment to table
COMMENT ON TABLE cars_standard IS 'Reference table for standardized car data';


-- =============================================
-- 2. CARS_UNIFIED TABLE
-- =============================================

CREATE TABLE IF NOT EXISTS cars_unified (
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key references (nullable)
    cars_standard_id BIGINT NULL,
    
    -- Source information
    source VARCHAR(20) NOT NULL CHECK (source IN ('carlistmy', 'mudahmy')),
    listing_url TEXT NOT NULL,
    
    -- Car details (normalized from both sources)
    condition VARCHAR(50) NULL,
    brand VARCHAR(100) NOT NULL,
    model_group VARCHAR(100) NULL,
    model VARCHAR(100) NOT NULL,
    variant VARCHAR(100) NULL,
    
    -- Car specifications
    year INTEGER NULL,
    mileage INTEGER NULL,
    transmission VARCHAR(50) NULL,
    seat_capacity VARCHAR(10) NULL,
    engine_cc VARCHAR(50) NULL,
    fuel_type VARCHAR(50) NULL,
    
    -- Pricing
    price INTEGER NULL,
    
    -- Location and contact
    location VARCHAR(255) NULL,
    
    -- Additional info
    information_ads TEXT NULL,
    images TEXT NULL, -- JSON field for image URLs
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    ads_tag VARCHAR(50) NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Timestamps
    last_scraped_at TIMESTAMP NULL,
    version INTEGER NOT NULL DEFAULT 1,
    sold_at TIMESTAMP NULL,
    last_status_check TIMESTAMP NULL,
    information_ads_date DATE NULL,
    
    -- Constraints
    CONSTRAINT unique_source_listing UNIQUE (source, listing_url)
);

-- Create indexes for cars_unified table
CREATE INDEX IF NOT EXISTS idx_cars_unified_source_listing ON cars_unified (source, listing_url);
CREATE INDEX IF NOT EXISTS idx_cars_unified_brand_model ON cars_unified (brand, model);
CREATE INDEX IF NOT EXISTS idx_cars_unified_price ON cars_unified (price);
CREATE INDEX IF NOT EXISTS idx_cars_unified_year ON cars_unified (year);
CREATE INDEX IF NOT EXISTS idx_cars_unified_last_scraped ON cars_unified (last_scraped_at);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ads_date ON cars_unified (information_ads_date);

-- Add comment to table
COMMENT ON TABLE cars_unified IS 'Unified car data from multiple sources (CarlistMY, MudahMY)';


-- =============================================
-- 3. PRICE_HISTORY_UNIFIED TABLE
-- =============================================

CREATE TABLE IF NOT EXISTS price_history_unified (
    id BIGSERIAL PRIMARY KEY,
    
    -- Source information
    source VARCHAR(20) NOT NULL DEFAULT 'carlistmy' 
        CHECK (source IN ('carlistmy', 'mudahmy')),
    
    -- Price tracking
    old_price INTEGER NULL,
    new_price INTEGER NOT NULL,
    
    -- Source reference for tracking
    listing_url TEXT NOT NULL,
    
    -- Timestamps
    changed_at TIMESTAMP NOT NULL,
    
    -- Constraints
    CONSTRAINT unique_listing_changed UNIQUE (listing_url, changed_at)
);

-- Create indexes for price_history_unified table
CREATE INDEX IF NOT EXISTS idx_price_history_listing_changed ON price_history_unified (listing_url, changed_at);
CREATE INDEX IF NOT EXISTS idx_price_history_changed_at ON price_history_unified (changed_at);

-- Add comment to table
COMMENT ON TABLE price_history_unified IS 'Unified price history tracking from multiple sources';


-- =============================================
-- 4. FOREIGN KEY CONSTRAINTS
-- =============================================

-- Foreign key from cars_unified to cars_standard
ALTER TABLE cars_unified 
ADD CONSTRAINT fk_cars_unified_standard 
FOREIGN KEY (cars_standard_id) REFERENCES cars_standard(id) ON DELETE CASCADE;


-- =============================================
-- 5. ADDITIONAL INDEXES FOR PERFORMANCE
-- =============================================

-- Additional performance indexes based on common queries
CREATE INDEX IF NOT EXISTS idx_cars_unified_status ON cars_unified (status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_cars_unified_deleted ON cars_unified (is_deleted) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_cars_unified_source ON cars_unified (source);

-- Composite indexes for common filter combinations
CREATE INDEX IF NOT EXISTS idx_cars_unified_brand_year ON cars_unified (brand, year);
CREATE INDEX IF NOT EXISTS idx_cars_unified_price_year ON cars_unified (price, year);

-- Price history performance indexes
CREATE INDEX IF NOT EXISTS idx_price_history_source ON price_history_unified (source);
CREATE INDEX IF NOT EXISTS idx_price_history_new_price ON price_history_unified (new_price);


-- =============================================
-- 6. SAMPLE QUERIES (Commented out)
-- =============================================

/*
-- Sample query to get today's car data
SELECT COUNT(*) FROM cars_unified 
WHERE information_ads_date = CURRENT_DATE;

-- Sample query to get price changes today
SELECT * FROM price_history_unified 
WHERE changed_at >= CURRENT_DATE 
ORDER BY changed_at DESC;

-- Sample query to get cars by brand and price range
SELECT brand, model, price, year, location 
FROM cars_unified 
WHERE brand = 'Honda' 
  AND price BETWEEN 50000 AND 100000 
  AND is_deleted = FALSE 
  AND status = 'active'
ORDER BY price;

-- Sample query to get standardized car data
SELECT brand_norm, model_norm, variant_norm, COUNT(*) as usage_count
FROM cars_standard cs
LEFT JOIN cars_unified cu ON cu.cars_standard_id = cs.id
GROUP BY brand_norm, model_norm, variant_norm
ORDER BY usage_count DESC;

-- Sample query to join unified cars with standard reference
SELECT cu.brand, cu.model, cu.price, cs.brand_norm, cs.model_norm, cs.variant_norm
FROM cars_unified cu
LEFT JOIN cars_standard cs ON cu.cars_standard_id = cs.id
WHERE cu.price IS NOT NULL
ORDER BY cu.price DESC
LIMIT 10;
*/

-- End of DDL Script