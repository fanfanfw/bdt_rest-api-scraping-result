-- Create carsome table for Carsome data
-- This table will store scraped data from Carsome website

CREATE TABLE IF NOT EXISTS carsome (
    id SERIAL PRIMARY KEY,
    image TEXT,
    brand VARCHAR(100),
    model VARCHAR(100),
    model_group VARCHAR(100) DEFAULT 'NO MODEL GROUP',
    variant VARCHAR(100),
    year INTEGER,
    mileage INTEGER,
    price INTEGER,
    created_at TIMESTAMP,
    cars_standard_id INTEGER REFERENCES cars_standard(id),

    -- Additional metadata fields
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active', -- active, sold, deleted
    is_deleted BOOLEAN DEFAULT FALSE,

    -- Source tracking
    source VARCHAR(50) DEFAULT 'carsome'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_carsome_brand_model ON carsome (brand, model);
CREATE INDEX IF NOT EXISTS idx_carsome_brand_model_variant ON carsome (brand, model, variant);
CREATE INDEX IF NOT EXISTS idx_carsome_model_group ON carsome (model_group);
CREATE INDEX IF NOT EXISTS idx_carsome_price ON carsome (price);
CREATE INDEX IF NOT EXISTS idx_carsome_year ON carsome (year);
CREATE INDEX IF NOT EXISTS idx_carsome_mileage ON carsome (mileage);
CREATE INDEX IF NOT EXISTS idx_carsome_created_at ON carsome (created_at);
CREATE INDEX IF NOT EXISTS idx_carsome_cars_standard_id ON carsome (cars_standard_id);
CREATE INDEX IF NOT EXISTS idx_carsome_status ON carsome (status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_carsome_brand_year ON carsome (brand, year);
CREATE INDEX IF NOT EXISTS idx_carsome_price_year ON carsome (price, year);

-- Add comments for documentation
COMMENT ON TABLE carsome IS 'Carsome scraped car data for price estimation';
COMMENT ON COLUMN carsome.cars_standard_id IS 'Foreign key to cars_standard for standardized car reference';
COMMENT ON COLUMN carsome.status IS 'Car listing status: active, sold, deleted';
COMMENT ON COLUMN carsome.source IS 'Data source identifier';
COMMENT ON COLUMN carsome.model_group IS 'Model group classification for categorization';