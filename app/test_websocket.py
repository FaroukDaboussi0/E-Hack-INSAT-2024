import asyncio
import websockets
import json

async def listen_to_ws():
    uri = "ws://localhost:8080/ws"  
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected to gas monitoring WebSocket.")

            while True:
                msg = await websocket.recv()
                data = json.loads(msg)

                print("\n📡 Received Data:")
                print(json.dumps(data, indent=4))
                
    except Exception as e:
        print("❌ Error connecting to WebSocket:", e)

if __name__ == "__main__":
    asyncio.run(listen_to_ws())
