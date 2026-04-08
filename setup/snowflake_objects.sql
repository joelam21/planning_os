/*
  snowflake_objects.sql
  -------------------------------------------------------
  Purpose:
    Creates the Snowflake infrastructure required to run
    the planning_os project — warehouse, database, and
    schemas. Run as SYSADMIN or ACCOUNTADMIN before
    running snowflake_roles.sql or the ingestion pipeline.

  Current implementation note:
    The ingestion pipeline currently loads source data
    directly into the DEV schema. The RAW schema below
    reflects the intended production architecture where
    raw source data lands separately before dbt
    transformation. Separating RAW and DEV enforces
    ELT boundaries more strictly and is a documented
    next step for this project.

  Schemas created:
    PLANNING_OS.RAW  — intended landing zone for raw 
                       source data before transformation
    PLANNING_OS.DEV  — dbt transformation output
                       (staging, intermediate, marts)
    PLANNING_OS.CI   — isolated schema for CI/CD runs

  Run order:
    1. snowflake_objects.sql  (this file)
    2. snowflake_roles.sql    (role and privilege setup)
    3. dbt debug              (validate dbt connection)
    4. run.sh pipeline        (ingestion and transformation)

  Notes:
    - IF NOT EXISTS guards make this script safe to re-run
    - Warehouse auto-suspend is set to 60 seconds for
      cost efficiency on a development account
    - Adjust warehouse size and retention for production
*/

-- =====================================================
-- 1. CREATE ROLES
-- =====================================================

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS PLANNING_OS_ADMIN
    COMMENT = 'Full DDL and DML access for planning_os project administration';

CREATE ROLE IF NOT EXISTS PLANNING_OS_DEV
    COMMENT = 'Development access — dbt runs, ingestion, analysis';

CREATE ROLE IF NOT EXISTS PLANNING_OS_CI
    COMMENT = 'CI/CD access — scoped to CI schema only';

CREATE ROLE IF NOT EXISTS PLANNING_OS_READ
    COMMENT = 'Read-only access for analysts and stakeholders';

-- =====================================================
-- 2. ROLE HIERARCHY
-- =====================================================

-- Admin inherits dev privileges
GRANT ROLE PLANNING_OS_DEV TO ROLE PLANNING_OS_ADMIN;

-- =====================================================
-- 3. WAREHOUSE PRIVILEGES
-- =====================================================

GRANT USAGE ON WAREHOUSE IDENTIFIER('compute_wh') 
    TO ROLE PLANNING_OS_ADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER('compute_wh') 
    TO ROLE PLANNING_OS_DEV;
GRANT USAGE ON WAREHOUSE IDENTIFIER('compute_wh') 
    TO ROLE PLANNING_OS_CI;
GRANT USAGE ON WAREHOUSE IDENTIFIER('compute_wh') 
    TO ROLE PLANNING_OS_READ;

-- =====================================================
-- 4. DATABASE PRIVILEGES
-- =====================================================

GRANT USAGE ON DATABASE PLANNING_OS 
    TO ROLE PLANNING_OS_ADMIN;
GRANT USAGE ON DATABASE PLANNING_OS 
    TO ROLE PLANNING_OS_DEV;
GRANT USAGE ON DATABASE PLANNING_OS 
    TO ROLE PLANNING_OS_CI;
GRANT USAGE ON DATABASE PLANNING_OS 
    TO ROLE PLANNING_OS_READ;

-- =====================================================
-- 5. SCHEMA PRIVILEGES — DEV
-- =====================================================

-- Admin and Dev: full access to DEV schema
GRANT ALL PRIVILEGES ON SCHEMA PLANNING_OS.DEV 
    TO ROLE PLANNING_OS_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PLANNING_OS.DEV 
    TO ROLE PLANNING_OS_DEV;

-- Read-only: usage on DEV schema
GRANT USAGE ON SCHEMA PLANNING_OS.DEV 
    TO ROLE PLANNING_OS_READ;

-- Future object grants — DEV schema
GRANT ALL PRIVILEGES ON FUTURE TABLES 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_DEV;
GRANT ALL PRIVILEGES ON FUTURE VIEWS 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_DEV;
GRANT SELECT ON FUTURE TABLES 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_READ;
GRANT SELECT ON FUTURE VIEWS 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_READ;

-- Existing object grants — DEV schema
GRANT ALL PRIVILEGES ON ALL TABLES 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_DEV;
GRANT ALL PRIVILEGES ON ALL VIEWS 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_DEV;
GRANT SELECT ON ALL TABLES 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_READ;
GRANT SELECT ON ALL VIEWS 
    IN SCHEMA PLANNING_OS.DEV TO ROLE PLANNING_OS_READ;

-- =====================================================
-- 6. SCHEMA PRIVILEGES — RAW
-- =====================================================

-- Admin and Dev: full access to RAW schema
GRANT ALL PRIVILEGES ON SCHEMA PLANNING_OS.RAW 
    TO ROLE PLANNING_OS_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PLANNING_OS.RAW 
    TO ROLE PLANNING_OS_DEV;

-- Future object grants — RAW schema
GRANT ALL PRIVILEGES ON FUTURE TABLES 
    IN SCHEMA PLANNING_OS.RAW TO ROLE PLANNING_OS_DEV;

-- Existing object grants — RAW schema
GRANT ALL PRIVILEGES ON ALL TABLES 
    IN SCHEMA PLANNING_OS.RAW TO ROLE PLANNING_OS_DEV;

-- =====================================================
-- 7. SCHEMA PRIVILEGES — CI
-- =====================================================

-- Create CI schema if it doesn't exist
-- (run as SYSADMIN or ACCOUNTADMIN)
CREATE SCHEMA IF NOT EXISTS PLANNING_OS.CI;

-- CI role: full access to CI schema only
GRANT ALL PRIVILEGES ON SCHEMA PLANNING_OS.CI 
    TO ROLE PLANNING_OS_CI;
GRANT ALL PRIVILEGES ON FUTURE TABLES 
    IN SCHEMA PLANNING_OS.CI TO ROLE PLANNING_OS_CI;
GRANT ALL PRIVILEGES ON FUTURE VIEWS 
    IN SCHEMA PLANNING_OS.CI TO ROLE PLANNING_OS_CI;

-- =====================================================
-- 8. ASSIGN ROLES TO USERS
-- =====================================================

-- Replace YOUR_USERNAME with your actual Snowflake username
-- GRANT ROLE PLANNING_OS_ADMIN TO USER YOUR_USERNAME;
-- GRANT ROLE PLANNING_OS_DEV TO USER YOUR_USERNAME;

-- For CI service account (if using one):
-- GRANT ROLE PLANNING_OS_CI TO USER CI_SERVICE_USER;

-- =====================================================
-- 9. VERIFY GRANTS
-- =====================================================

-- Run these to confirm grants landed correctly:
-- SHOW GRANTS TO ROLE PLANNING_OS_DEV;
-- SHOW GRANTS TO ROLE PLANNING_OS_CI;
-- SHOW GRANTS TO ROLE PLANNING_OS_READ;
-- SHOW FUTURE GRANTS IN SCHEMA PLANNING_OS.DEV;