cat > .env.example << 'EOF'
# Non-secret config (copy to .env)
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=

DBT_TARGET=dev
DBT_PROFILES_DIR=./dbt
EOF

cp -n .env.example .env

cat > .gitignore << 'EOF'
# Python
.venv/
__pycache__/
*.pyc

# Local env
.env

# OS
.DS_Store
EOF