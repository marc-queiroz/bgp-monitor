from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis
from motor.motor_asyncio import AsyncIOMotorClient
import json

app = FastAPI()
redis = Redis.from_url("redis://redis:6379")
MONGO_URI = "mongodb://root:example@mongodb:27017/"
MONGO_DB = "bgp_data"
MONGO_COLLECTION = "prefix_relations"

@app.get("/peer/{peer_asn}")
async def get_peer(peer_asn: str):
    data = await redis.hgetall(f"bgp:peer:{peer_asn}")
    return {k.decode(): json.loads(v) for k, v in data.items()}

@app.get("/prefix/{prefix}")
async def get_prefix(prefix: str):
    keys = await redis.keys("bgp:peer:*")
    results = {}
    for key in keys:
        peer_asn = key.decode().split(":")[2]
        value = await redis.hget(key, prefix)
        if value:
            results[peer_asn] = json.loads(value)
    return results

@app.get("/all")
async def get_all():
    keys = await redis.keys("bgp:peer:*")
    all_data = {}
    for key in keys:
        peer_asn = key.decode().split(":")[2]
        data = await redis.hgetall(key)
        all_data[peer_asn] = {k.decode(): json.loads(v) for k, v in data.items()}
    return all_data

def convert_objectid(doc):
    """Converte ObjectId para string e remove o campo _id"""
    if '_id' in doc:
        doc['id'] = str(doc['_id'])
        del doc['_id']
    return doc

@app.get("/origin/{asn}")
async def get_origin(asn: str):
    mongo_client = None
    try:
        # Validar ASN primeiro, antes de qualquer operação
        try:
            target_asn = int(asn)
        except ValueError:
            raise HTTPException(status_code=400, detail="ASN inválido. Deve ser um número inteiro")
        
        except HTTPException:
            raise  # Re-lança imediatamente exceções de validação

        # Conexão MongoDB após validação bem-sucedida
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

        # Pipeline de agregação otimizado
        pipeline = [
            {
                "$match": {
                    "origin_as": target_asn,
                    "announcement": True
                }
            },
            {
                "$sort": {"timestamp": -1}
            },
            {
                "$group": {
                    "_id": "$prefix",
                    "latest": {"$first": "$$ROOT"}
                }
            },
            {
                "$replaceRoot": {"newRoot": "$latest"}
            },
            {
                "$project": {
                    "_id": 0,  # Exclui o campo ObjectId
                    "peer_asn": 1,
                    "prefix": 1,
                    "as_path": 1,
                    "communities": 1,
                    "origin_as": 1,
                    "timestamp": 1
                }
            }
        ]

        # Executar consulta
        cursor = collection.aggregate(pipeline)
        results = [convert_objectid(doc) async for doc in cursor]

        return results

    except HTTPException:
        # Re-lançar exceções HTTP já tratadas
        raise
    except Exception as e:
        # Capturar outros erros genéricos
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        # Fechar conexão apenas se foi aberta
        if mongo_client and isinstance(mongo_client, AsyncIOMotorClient):
            mongo_client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
