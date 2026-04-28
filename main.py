import asyncio
import json
import websockets

# Store connected devices: device_id -> websocket
connected_devices = {}

# Simple helper: compute datacheck same way as client
def make_datacheck(data):
    return len(json.dumps(data))

async def handle_client(websocket, path):
    print("New connection")
    device_id = None

    try:
        async for message in websocket:
            try:
                packet = json.loads(message)
            except json.JSONDecodeError:
                print("Invalid JSON, ignoring")
                continue

            # Expecting: [LOCATION] [ROUTER_ID] [DEVICE_ID] [SOURCE_ID] [DATA] [DATACHECK]
            location = packet.get("location")
            router_id = packet.get("routerId")
            device_id_target = packet.get("deviceId")
            source_id = packet.get("sourceId")
            data = packet.get("data")
            datacheck = packet.get("datacheck")

            # Verify datacheck
            expected = make_datacheck(data)
            if expected != datacheck:
                print("Bad datacheck, dropping packet:", packet)
                continue

            # Handle registration
            if isinstance(data, dict) and data.get("type") == "register":
                device_id = data.get("deviceId")
                if device_id is not None:
                    connected_devices[device_id] = websocket
                    print(f"Device {device_id} registered")
                    # send ack
                    ack_packet = {
                        "location": location,
                        "routerId": router_id,
                        "deviceId": device_id,
                        "sourceId": 0,  # server
                        "data": {"type": "register-ok", "deviceId": device_id},
                        "datacheck": make_datacheck({"type": "register-ok", "deviceId": device_id})
                    }
                    await websocket.send(json.dumps(ack_packet))
                continue

            print(f"Packet from {source_id} to {device_id_target}: {data}")

            # Route packet to target device if connected
            target_ws = connected_devices.get(device_id_target)
            if target_ws is not None and target_ws.open:
                try:
                    await target_ws.send(json.dumps(packet))
                    print(f"Forwarded packet to device {device_id_target}")
                except Exception as e:
                    print(f"Error forwarding to {device_id_target}:", e)
            else:
                print(f"Device {device_id_target} not connected")

    except websockets.exceptions.ConnectionClosed:
        print("Connection closed")
    finally:
        # Clean up disconnected device
        if device_id is not None and connected_devices.get(device_id) is websocket:
            del connected_devices[device_id]
            print(f"Device {device_id} removed")


async def main():
    # Replit usually exposes a PORT env var, but default to 8000
    import os
    port = int(os.environ.get("PORT", 8000))
    async with websockets.serve(handle_client, "0.0.0.0", port):
        print(f"Server running on port {port}")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
