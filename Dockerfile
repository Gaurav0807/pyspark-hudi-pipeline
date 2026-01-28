# Use a slim Python image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install boto3


# Copy the application source code
# The source code is expected to be in a directory named 'src'
COPY src/ /app




