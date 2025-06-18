# Use the official Python 3.12 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy code
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Cloud Run expects
ENV PORT=8080
EXPOSE 8080

# Set the entrypoint for Cloud Run
CMD ["gunicorn", "-b", ":8080", "main:sales_data"]
