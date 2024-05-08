# MongoDB Docker Setup

This README provides detailed instructions on how to deploy a MongoDB database using Docker Compose and load movies data.

## Prerequisites

Before you begin, make sure you have the following installed on your system:
- Docker
- Docker Compose
- Python 3: Ensure Python 3 is installed on your system.
- `pymongo`: The Python MongoDB driver. You can install it using pip:
```bash
pip install pymongo
```

You can download Docker Desktop which includes Docker Compose from [Docker's official website](https://www.docker.com/products/docker-desktop).

## Execution

1. Start Docker Engine
2. In this directory execute in terminal `docker compose up -d`
3. Load movies.json data via python script `mongo-task.py`
