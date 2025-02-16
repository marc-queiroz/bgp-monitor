import asyncio
import json
import websockets
import pprint
from redis.asyncio import Redis
from motor.motor_asyncio import AsyncIOMotorClient

# Configurações
REDIS_URL = "redis://redis:6379"
RIS_WEBSOCKET_URL = "wss://ris-live.ripe.net/v1/ws/?client=my-bgp-client-v1"
PEER_ASNS = {"9002"}
RECONNECT_DELAY = 10

MONGO_URI = "mongodb://root:example@mongodb:27017/"
MONGO_DB = "bgp_data"
MONGO_COLLECTION = "prefix_relations"

async def handle_message(message: str, redis: Redis, mongo_collection):
    try:
        data = json.loads(message)
        print("Mensagem recebida:\n")
        pprint.pprint(data, indent=4)
        
        if data.get("type") != "ris_message":
            return

        msg_data = data.get("data", {})
        
        if msg_data.get("type") != "UPDATE":
            return

        peer_asn = str(msg_data.get("peer_asn", ""))
        print(f"Processando ASN: {peer_asn}")

        # Processar Redis
        bgp_entry = {
            "as_path": msg_data.get("path", []),
            "community": [f"{c[0]}:{c[1]}" for c in msg_data.get("community", [])],
            "origin": msg_data.get("origin", ""),
            "next_hop": msg_data.get("announcements", [{}])[0].get("next_hop", "") if msg_data.get("announcements") else "",
            "timestamp": msg_data.get("timestamp", ""),
            "peer_ip": msg_data.get("peer", "")
        }

        async with redis.pipeline() as pipe:
            # Processar anúncios e retiradas
            for announcement in msg_data.get("announcements", []):
                for prefix in announcement.get("prefixes", []):
                    await pipe.hset(
                        f"bgp:peer:{peer_asn}",
                        prefix,
                        json.dumps(bgp_entry)
                    )
                    print(f"Armazenado no Redis: {prefix}")

            for prefix in msg_data.get("withdrawals", []):
                await pipe.hdel(f"bgp:peer:{peer_asn}", prefix)
                print(f"Removido do Redis: {prefix}")

            await pipe.execute()

        # Processar MongoDB
        as_path = msg_data.get("path", [])
        origin_as = as_path[-1] if as_path else None
        
        for announcement in msg_data.get("announcements", []):
            for prefix in announcement.get("prefixes", []):
                document = {
                    "prefix": prefix,
                    "as_path": as_path,
                    "origin_as": origin_as,
                    "timestamp": msg_data.get("timestamp", ""),
                    "peer_asn": peer_asn,
                    "communities": [f"{c[0]}:{c[1]}" for c in msg_data.get("community", [])],
                    "announcement": True
                }
                
                await mongo_collection.update_one(
                    {"prefix": prefix, "peer_asn": peer_asn},
                    {"$set": document},
                    upsert=True
                )
                print(f"Armazenado no MongoDB: {prefix}")

        # Processar retiradas no MongoDB
        for prefix in msg_data.get("withdrawals", []):
            await mongo_collection.update_one(
                {"prefix": prefix, "peer_asn": peer_asn},
                {"$set": {"announcement": False, "timestamp": msg_data.get("timestamp", "")}},
                upsert=True
            )
            print(f"Marcado como retirado no MongoDB: {prefix}")

    except Exception as e:
        print(f"Erro ao processar mensagem: {str(e)}")
        import traceback
        traceback.print_exc()

async def maintain_connection():
    while True:
        try:
            async with websockets.connect(
                RIS_WEBSOCKET_URL,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=5
            ) as websocket:
                print("Conectado ao RIS Live")
                
                # Configurar clients
                redis = await Redis.from_url(
                    REDIS_URL,
                    socket_connect_timeout=5,
                    socket_keepalive=True
                )
                
                mongo_client = AsyncIOMotorClient(MONGO_URI)
                mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

                # Criar índices
                await mongo_collection.create_index("prefix")
                await mongo_collection.create_index("origin_as")
                await mongo_collection.create_index("peer_asn")
                
                # Subscrever
                subscription = {
                    "type": "ris_subscribe",
                    "data": {
                        "moreSpecific": True,
                        "socketOptions": {
                            "includeRaw": False
                        }
                    }
                }
                await websocket.send(json.dumps(subscription))
                
                # Processar mensagens
                async for message in websocket:
                    await handle_message(message, redis, mongo_collection)
                    
        except (websockets.ConnectionClosed, ConnectionResetError) as e:
            print(f"Conexão fechada: {str(e)}, reconectando em {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            print(f"Erro grave: {str(e)}")
            await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    asyncio.run(maintain_connection())