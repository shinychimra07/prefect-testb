#!/bin/bash
set -e

# TestBot Setup Script for Prefect
# Builds the server from local source and starts all services

cd "$(dirname "$0")"

echo "Building Prefect server from local source..."
docker compose build --no-cache --pull prefect-server

echo "Starting Prefect services..."
docker compose up -d --wait

echo "Ensuring all database tables exist..."
docker compose exec -T prefect-server uv run python3 /opt/prefect/ensure-tables.py

echo "Creating sample flows..."
# Create Python script for sample flows
docker compose exec -T prefect-server bash << 'EOF'
export PREFECT_API_URL=http://localhost:4200/api

uv run python3 << 'PYEOF'
from prefect import flow, task

@task
def add_numbers(x: int, y: int):
    return x + y

@flow(name="TestBot Sample Flow")
def sample_flow(x: int = 10, y: int = 5):
    result = add_numbers(x, y)
    return result

@flow(name="Hello World Flow")
def hello_flow(name: str = "TestBot"):
    return f"Hello {name}!"

if __name__ == "__main__":
    try:
        sample_flow()
        hello_flow()
        print("Flows registered successfully")
    except Exception as e:
        print(f"Warning: {e}")
PYEOF
EOF

echo "✓ Prefect setup complete"
