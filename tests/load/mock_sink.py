from fastapi import FastAPI, Response
import uvicorn

app = FastAPI()


@app.post("/webhook-sink")
async def sink():
    # Return 200 OK immediately with no body to save bandwidth
    return Response(status_code=200)


if __name__ == "__main__":
    # Run with multiple workers to match your Legion's CPU cores
    # This ensures the sink never becomes the bottleneck
    uvicorn.run(
        "mock_sink:app", host="0.0.0.0", port=8080, workers=4, log_level="warning"
    )
