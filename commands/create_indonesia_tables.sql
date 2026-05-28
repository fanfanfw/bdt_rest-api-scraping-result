-- PostgreSQL DDL for isolated Indonesia unified car tables.
-- Safe to re-run: tables, indexes, comments, and FK constraint are idempotent.

-- =============================================
-- 1. CARS_UNIFIED_IND TABLE
-- =============================================

CREATE TABLE IF NOT EXISTS cars_unified_ind (
    id BIGSERIAL PRIMARY KEY,

    -- Foreign key reference to normalized car data (nullable)
    cars_standard_id BIGINT NULL,

    -- Source and country information
    source VARCHAR(20) NOT NULL,
    country_code VARCHAR(2) NOT NULL DEFAULT 'ID',
    currency_code VARCHAR(3) NOT NULL DEFAULT 'IDR',
    listing_url TEXT NOT NULL,
    listing_id TEXT NULL,

    -- Car details
    condition VARCHAR(50) NULL,
    brand VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    variant VARCHAR(100) NULL,
    series VARCHAR(100) NULL,
    type VARCHAR(100) NULL,

    -- Car specifications
    year INTEGER NULL,
    mileage INTEGER NULL,
    transmission VARCHAR(50) NULL,
    seat_capacity VARCHAR(50) NULL,
    engine_cc VARCHAR(50) NULL,
    fuel_type VARCHAR(50) NULL,

    -- Pricing (Indonesia Rupiah values can exceed INTEGER)
    price BIGINT NULL,

    -- Location and contact
    location VARCHAR(255) NULL,
    seller_name TEXT NULL,
    whatsapp_number TEXT[] NULL,
    contact_seller TEXT NULL,

    -- Additional info
    information_ads TEXT NULL,
    images TEXT[] NULL,

    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'active',

    -- Timestamps
    created_at TIMESTAMPTZ NULL,
    last_scraped_at TIMESTAMPTZ NULL,
    version INTEGER NOT NULL DEFAULT 1,
    information_ads_date DATE NULL,

    CONSTRAINT unique_source_listing_ind UNIQUE (source, listing_url)
);

COMMENT ON TABLE cars_unified_ind IS 'Unified Indonesia car data from Mobil123, Carmudi, OLX, and CarsomeID';

-- Idempotent constraint additions keep this script safe to re-run when tables already exist.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'cars_unified_ind'
          AND column_name = 'seat_capacity'
          AND (data_type <> 'character varying' OR character_maximum_length <> 50)
    ) THEN
        ALTER TABLE cars_unified_ind
        ALTER COLUMN seat_capacity TYPE VARCHAR(50);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_cars_unified_ind_source'
          AND conrelid = 'cars_unified_ind'::regclass
    ) THEN
        ALTER TABLE cars_unified_ind
        ADD CONSTRAINT chk_cars_unified_ind_source
        CHECK (source IN ('mobil123', 'carmudi', 'olx', 'carsomeid'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_cars_unified_ind_country_code'
          AND conrelid = 'cars_unified_ind'::regclass
    ) THEN
        ALTER TABLE cars_unified_ind
        ADD CONSTRAINT chk_cars_unified_ind_country_code
        CHECK (country_code = 'ID');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_cars_unified_ind_currency_code'
          AND conrelid = 'cars_unified_ind'::regclass
    ) THEN
        ALTER TABLE cars_unified_ind
        ADD CONSTRAINT chk_cars_unified_ind_currency_code
        CHECK (currency_code = 'IDR');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_cars_unified_ind_standard'
          AND conrelid = 'cars_unified_ind'::regclass
    ) THEN
        ALTER TABLE cars_unified_ind
        ADD CONSTRAINT fk_cars_unified_ind_standard
        FOREIGN KEY (cars_standard_id) REFERENCES cars_standard(id) ON DELETE SET NULL;
    END IF;
END
$$;

-- Indexes comparable to cars_unified plus Indonesia country/source access patterns.
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_source_listing ON cars_unified_ind (source, listing_url);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_brand_model ON cars_unified_ind (brand, model);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_price ON cars_unified_ind (price);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_year ON cars_unified_ind (year);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_last_scraped ON cars_unified_ind (last_scraped_at);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_ads_date ON cars_unified_ind (information_ads_date);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_status ON cars_unified_ind (status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_source ON cars_unified_ind (source);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_brand_year ON cars_unified_ind (brand, year);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_price_year ON cars_unified_ind (price, year);
CREATE INDEX IF NOT EXISTS idx_cars_unified_ind_country_source ON cars_unified_ind (country_code, source);


-- =============================================
-- 2. PRICE_HISTORY_UNIFIED_IND TABLE
-- =============================================

CREATE TABLE IF NOT EXISTS price_history_unified_ind (
    id BIGSERIAL PRIMARY KEY,

    -- Source and country information
    source VARCHAR(20) NOT NULL,
    country_code VARCHAR(2) NOT NULL DEFAULT 'ID',
    currency_code VARCHAR(3) NOT NULL DEFAULT 'IDR',
    listing_id TEXT NULL,

    -- Price tracking
    old_price BIGINT NULL,
    new_price BIGINT NOT NULL,

    -- Source reference for tracking
    listing_url TEXT NOT NULL,

    -- Timestamps
    changed_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT unique_listing_changed_ind UNIQUE (listing_url, changed_at)
);

COMMENT ON TABLE price_history_unified_ind IS 'Unified Indonesia price history from Mobil123, Carmudi, OLX, and CarsomeID';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_price_history_unified_ind_source'
          AND conrelid = 'price_history_unified_ind'::regclass
    ) THEN
        ALTER TABLE price_history_unified_ind
        ADD CONSTRAINT chk_price_history_unified_ind_source
        CHECK (source IN ('mobil123', 'carmudi', 'olx', 'carsomeid'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_price_history_unified_ind_country_code'
          AND conrelid = 'price_history_unified_ind'::regclass
    ) THEN
        ALTER TABLE price_history_unified_ind
        ADD CONSTRAINT chk_price_history_unified_ind_country_code
        CHECK (country_code = 'ID');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_price_history_unified_ind_currency_code'
          AND conrelid = 'price_history_unified_ind'::regclass
    ) THEN
        ALTER TABLE price_history_unified_ind
        ADD CONSTRAINT chk_price_history_unified_ind_currency_code
        CHECK (currency_code = 'IDR');
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_price_history_ind_listing_changed ON price_history_unified_ind (listing_url, changed_at);
CREATE INDEX IF NOT EXISTS idx_price_history_ind_changed_at ON price_history_unified_ind (changed_at);
CREATE INDEX IF NOT EXISTS idx_price_history_ind_source ON price_history_unified_ind (source);
CREATE INDEX IF NOT EXISTS idx_price_history_ind_new_price ON price_history_unified_ind (new_price);
CREATE INDEX IF NOT EXISTS idx_price_history_ind_country_source ON price_history_unified_ind (country_code, source);
