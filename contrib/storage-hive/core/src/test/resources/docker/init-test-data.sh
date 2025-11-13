#!/bin/bash
# Initialize Hive test data for Apache Drill tests
# This script runs inside the Hive Docker container

set -e

echo "=========================================="
echo "Hive Test Data Initialization Starting..."
echo "=========================================="

# Wait for HiveServer2 to be ready
echo "Waiting for HiveServer2 to accept JDBC connections..."
MAX_RETRIES=60
RETRY_COUNT=0
until beeline -u jdbc:hive2://localhost:10000 -e "show databases;" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HiveServer2 failed to accept connections within timeout"
        exit 1
    fi
    echo "Waiting for HiveServer2... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 3
done
echo "✓ HiveServer2 is ready and accepting connections!"

# Create test databases and tables using beeline
echo "Creating test databases and tables..."
beeline -u jdbc:hive2://localhost:10000 --silent=true <<'EOF'

-- ============================================
-- Basic Tables
-- ============================================
CREATE DATABASE IF NOT EXISTS default;
USE default;

-- Simple key-value table
CREATE TABLE IF NOT EXISTS kv(key INT, value STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO kv VALUES
  (1, 'value_1'), (2, 'value_2'), (3, 'value_3'),
  (4, 'value_4'), (5, 'value_5');

-- Create db1 for multi-database tests
CREATE DATABASE IF NOT EXISTS db1;

-- Table in db1 with RegEx SerDe
CREATE TABLE IF NOT EXISTS db1.kv_db1(key STRING, value STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO db1.kv_db1 VALUES
  ('1', 'value_1'), ('2', 'value_2'), ('3', 'value_3');

-- Empty table for testing
CREATE TABLE IF NOT EXISTS empty_table(a INT, b STRING);

-- ============================================
-- readtest: Table with various data types
-- ============================================
CREATE TABLE IF NOT EXISTS readtest (
  binary_field BINARY,
  boolean_field BOOLEAN,
  tinyint_field TINYINT,
  decimal0_field DECIMAL,
  decimal9_field DECIMAL(6, 2),
  decimal18_field DECIMAL(15, 5),
  decimal28_field DECIMAL(23, 1),
  decimal38_field DECIMAL(30, 3),
  double_field DOUBLE,
  float_field FLOAT,
  int_field INT,
  bigint_field BIGINT,
  smallint_field SMALLINT,
  string_field STRING,
  varchar_field VARCHAR(50),
  timestamp_field TIMESTAMP,
  date_field DATE,
  char_field CHAR(10)
) PARTITIONED BY (
  boolean_part BOOLEAN,
  tinyint_part TINYINT,
  decimal0_part DECIMAL,
  decimal9_part DECIMAL(6, 2),
  decimal18_part DECIMAL(15, 5),
  decimal28_part DECIMAL(23, 1),
  decimal38_part DECIMAL(30, 3),
  double_part DOUBLE,
  float_part FLOAT,
  int_part INT,
  bigint_part BIGINT,
  smallint_part SMALLINT,
  string_part STRING,
  varchar_part VARCHAR(50),
  timestamp_part TIMESTAMP,
  date_part DATE,
  char_part CHAR(10)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ('serialization.null.format'='');

-- Insert sample data into readtest (partition 1)
INSERT INTO readtest PARTITION(
  boolean_part=true, tinyint_part=64, decimal0_part=36.9,
  decimal9_part=36.9, decimal18_part=3289379872.945645, decimal28_part=39579334534534.35345,
  decimal38_part=363945093845093890.9, double_part=8.345, float_part=4.67,
  int_part=123456, bigint_part=234235, smallint_part=3455,
  string_part='string', varchar_part='varchar',
  timestamp_part='2013-07-05 17:01:00', date_part='2013-07-05',
  char_part='char'
)
VALUES (
  cast('binary' as binary), true, 64, 36, 36.90, 3289379872.94565,
  39579334534534.4, 363945093845093890.900, 8.345, 4.67, 123456, 234235, 3455,
  'string', 'varchar', '2013-07-05 17:01:00', '2013-07-05', 'char'
);

-- Insert sample data into readtest (partition 2)
INSERT INTO readtest PARTITION(
  boolean_part=true, tinyint_part=65, decimal0_part=36.9,
  decimal9_part=36.9, decimal18_part=3289379872.945645, decimal28_part=39579334534534.35345,
  decimal38_part=363945093845093890.9, double_part=8.345, float_part=4.67,
  int_part=123456, bigint_part=234235, smallint_part=3455,
  string_part='string', varchar_part='varchar',
  timestamp_part='2013-07-05 17:01:00', date_part='2013-07-05',
  char_part='char'
)
VALUES (
  cast('binary' as binary), true, 65, 36, 36.90, 3289379872.94565,
  39579334534534.4, 363945093845093890.900, 8.345, 4.67, 123456, 234235, 3455,
  'string', 'varchar', '2013-07-05 17:01:00', '2013-07-05', 'char'
);

-- ============================================
-- infoschematest: All Hive types for INFORMATION_SCHEMA tests
-- ============================================
CREATE TABLE IF NOT EXISTS infoschematest(
  booleanType BOOLEAN,
  tinyintType TINYINT,
  smallintType SMALLINT,
  intType INT,
  bigintType BIGINT,
  floatType FLOAT,
  doubleType DOUBLE,
  dateType DATE,
  timestampType TIMESTAMP,
  binaryType BINARY,
  decimalType DECIMAL(38, 2),
  stringType STRING,
  varCharType VARCHAR(20),
  listType ARRAY<STRING>,
  mapType MAP<STRING,INT>,
  structType STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>,
  uniontypeType UNIONTYPE<int, double, array<string>>,
  charType CHAR(10)
);

-- ============================================
-- Parquet Tables
-- ============================================
CREATE TABLE IF NOT EXISTS kv_parquet(key INT, value STRING)
STORED AS PARQUET;

INSERT INTO kv_parquet SELECT * FROM kv;

CREATE TABLE IF NOT EXISTS readtest_parquet
STORED AS PARQUET
AS SELECT * FROM readtest;

-- ============================================
-- Views
-- ============================================
CREATE VIEW IF NOT EXISTS hive_view AS SELECT * FROM kv;
CREATE VIEW IF NOT EXISTS db1.hive_view AS SELECT * FROM db1.kv_db1;

EOF

echo "=========================================="
echo "✓ Test Data Initialization Completed!"
echo "=========================================="
echo "Created databases: default, db1"
echo "Created tables: kv, kv_db1, empty_table, readtest, infoschematest"
echo "Created parquet tables: kv_parquet, readtest_parquet"
echo "Created views: hive_view, db1.hive_view"
echo "=========================================="
