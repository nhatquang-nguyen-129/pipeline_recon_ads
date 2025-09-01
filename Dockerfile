# Use official lightweight Python image
FROM python:3.13-slim

# Set workdir
WORKDIR /app

# Copy dependency files
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Container entrypoint
ENTRYPOINT ["python", "main.py"]
