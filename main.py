from fastapi import FastAPI

# 1. Create the application instance
app = FastAPI(
    title="Webhook Engine",
    description="Learning reliable webhook delivery",
    version="0.1.0"
)

# 2. Define a "Route" (Endpoint)
# When someone visits the homepage (/)
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Webhook Engine!"}

# 3. Define a Health Check
# Used by monitoring tools to see if app is alive
@app.get("/health")
async def health_check():
    return {"status": "healthy"}