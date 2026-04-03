# 1. Start with a Python base
FROM python:3.12-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy your requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy all your project code into the container
COPY . .

# 5. This is the "Starting Key" - it runs the worker
CMD ["python", "main.py"]