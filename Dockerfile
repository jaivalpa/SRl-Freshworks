# Use Python 3.10 base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy project files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the Python path to include the app folder
ENV PYTHONPATH="/app"

# Expose the application port
EXPOSE 8080

# Command to run the application
CMD ["python", "app/main.py"]
