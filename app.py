import random
from flask import Flask, jsonify
import redis
import pymongo
import hazelcast

app = Flask(__name__)

# --- BAĞLANTILAR ---

# 1. Redis
try:
    r_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r_client.ping() # Test
except:
    print("Redis bağlantı hatası! Docker ayakta mı?")

# 2. MongoDB
try:
    # Docker varsayılanında auth kapalı olabilir veya url içine yazılır
    # Basit test için authsuz string deniyoruz:
    m_client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/")
    m_db = m_client["nosql_lab"]
    m_collection = m_db["students"]
except:
    print("MongoDB bağlantı hatası!")

# 3. Hazelcast
try:
    hz_client = hazelcast.HazelcastClient(
        cluster_members=["localhost:5701"],
        cluster_name="dev" # Varsayılan cluster adı genellikle 'dev'dir
    )
    hz_map = hz_client.get_map("students").blocking()
except:
    print("Hazelcast bağlantı hatası!")

# --- VERİ ÜRETİMİ ---
def init_data():
    print("Veriler yükleniyor (10.000 kayıt)...")
    
    # Temizlik
    try:
        r_client.flushall()
        m_collection.delete_many({})
        hz_map.clear()
    except:
        pass

    # Batch işlemi (Performans için)
    # Redis Pipeline
    pipe = r_client.pipeline()
    # Mongo Bulk
    mongo_docs = []
    
    for i in range(1, 10001):
        s_no = str(2025000000 + i)
        s_data = {
            "student_no": s_no,
            "name": f"Ogrenci {i}",
            "department": random.choice(["Ceng", "Music", "Art"])
        }
        
        # Hazırlık
        pipe.hset(s_no, mapping=s_data)
        mongo_docs.append(s_data)
        # Hazelcast (Tek tek yazmak zorunda python client ile)
        hz_map.put(s_no, s_data)
    
    # Toplu yazma
    pipe.execute()
    if mongo_docs:
        m_collection.insert_many(mongo_docs)
        
    print("Veri yükleme tamamlandı!")

# --- ENDPOINTLER ---

@app.route('/nosql-lab-rd/student_no=<student_no>', methods=['GET'])
def get_redis(student_no):
    return jsonify(r_client.hgetall(student_no))

@app.route('/nosql-lab-mon/student_no=<student_no>', methods=['GET'])
def get_mongo(student_no):
    data = m_collection.find_one({"student_no": student_no}, {"_id": 0})
    return jsonify(data)

@app.route('/nosql-lab-hz/student_no=<student_no>', methods=['GET'])
def get_hazelcast(student_no):
    return jsonify(hz_map.get(student_no))

if __name__ == '__main__':
    init_data() # İlk çalıştırmada verileri yükler
    app.run(port=8080, threaded=True)