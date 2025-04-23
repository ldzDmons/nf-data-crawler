-- 奶粉产品基本信息表
CREATE TABLE IF NOT EXISTS milk_products (
    id SERIAL PRIMARY KEY,                   -- 自增主键
    product_id INTEGER UNIQUE NOT NULL,      -- 产品ID
    name VARCHAR(255) NOT NULL,              -- 产品名称
    thumbnail TEXT,                          -- 缩略图URL
    thumbnail_alt TEXT,                      -- 缩略图替代文本
    click_count INTEGER,                     -- 点击次数
    price NUMERIC,                           -- 价格
    tag INTEGER,                             -- 标签ID
    tag_time BIGINT,                         -- 标签时间戳
    icon TEXT,                               -- 图标URL
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 创建时间
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()   -- 更新时间
);

-- 奶粉产品详情表
CREATE TABLE IF NOT EXISTS milk_product_details (
    id SERIAL PRIMARY KEY,                   -- 自增主键
    product_id INTEGER UNIQUE NOT NULL,      -- 产品ID，关联产品基本信息表
    brand VARCHAR(100),                      -- 品牌
    series VARCHAR(100),                     -- 系列
    origin VARCHAR(100),                     -- 产地
    milk_source VARCHAR(100),                -- 奶源
    age_range VARCHAR(100),                  -- 适用年龄
    manufacturer VARCHAR(255),               -- 厂家
    operator VARCHAR(255),                   -- 运营商
    specification VARCHAR(100),              -- 规格
    stage VARCHAR(50),                       -- 段位
    reference_price VARCHAR(100),            -- 参考价
    category VARCHAR(100),                   -- 类别
    version VARCHAR(100),                    -- 版本
    formula_registration VARCHAR(100),       -- 配方注册号
    formula_evaluation TEXT,                 -- 配方评价
    ingredients TEXT,                        -- 配料表
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 创建时间
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 更新时间
    FOREIGN KEY (product_id) REFERENCES milk_products(product_id) ON DELETE CASCADE
);

-- 奶粉产品营养成分表
CREATE TABLE IF NOT EXISTS milk_product_nutrients (
    id SERIAL PRIMARY KEY,                   -- 自增主键
    product_id INTEGER NOT NULL,             -- 产品ID，关联产品基本信息表
    nutrient_name VARCHAR(100) NOT NULL,     -- 营养成分名称
    content VARCHAR(100),                    -- 含量
    unit VARCHAR(50),                        -- 单位
    description TEXT,                        -- 描述
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 创建时间
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 更新时间
    FOREIGN KEY (product_id) REFERENCES milk_products(product_id) ON DELETE CASCADE,
    UNIQUE (product_id, nutrient_name)       -- 同一产品的营养成分名称不重复
);

-- 奶粉产品额外详情表
CREATE TABLE IF NOT EXISTS milk_product_extra_details (
    id SERIAL PRIMARY KEY,                   -- 自增主键
    product_id INTEGER NOT NULL,             -- 产品ID，关联产品基本信息表
    key VARCHAR(100) NOT NULL,               -- 键名
    value TEXT,                              -- 值
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 创建时间
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),  -- 更新时间
    FOREIGN KEY (product_id) REFERENCES milk_products(product_id) ON DELETE CASCADE,
    UNIQUE (product_id, key)                 -- 同一产品的键名不重复
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_milk_products_product_id ON milk_products(product_id);
CREATE INDEX IF NOT EXISTS idx_milk_product_details_product_id ON milk_product_details(product_id);
CREATE INDEX IF NOT EXISTS idx_milk_product_nutrients_product_id ON milk_product_nutrients(product_id);
CREATE INDEX IF NOT EXISTS idx_milk_product_extra_details_product_id ON milk_product_extra_details(product_id);

-- 创建触发器函数，自动更新updated_at字段
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- 为各表创建触发器
CREATE TRIGGER update_milk_products_updated_at
BEFORE UPDATE ON milk_products
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_milk_product_details_updated_at
BEFORE UPDATE ON milk_product_details
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_milk_product_nutrients_updated_at
BEFORE UPDATE ON milk_product_nutrients
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_milk_product_extra_details_updated_at
BEFORE UPDATE ON milk_product_extra_details
FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column(); 