FROM python:3.9.1

# Update system
RUN apt-get update

# Install Python packages
RUN pip install pandas sqlalchemy psycopg2 requests

# Set working directory
WORKDIR /app

# Copy python scripts
COPY data_ingest.py data_ingest.py

ENTRYPOINT ["python", "data_ingest.py"]