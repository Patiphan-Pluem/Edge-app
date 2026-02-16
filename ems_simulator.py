import os
import time
import psycopg2
import psutil

DB_HOST = os.getenv('DB_HOST', 'timescaledb-service.edge-apps')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_NAME = os.getenv('DB_NAME', 'postgres')

SERVICE_NAME = "ems-simulator"

print(f"--- Starting EMS ---")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"[DB Error] Connection failed: {e}")
        return None

def init_metrics_db():
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_metrics (
                time TIMESTAMPTZ NOT NULL,
                service_name TEXT,
                cpu_percent DOUBLE PRECISION,
                memory_mb DOUBLE PRECISION
            );
        """)
        try:
            cur.execute("SELECT create_hypertable('system_metrics', 'time', if_not_exists => TRUE);")
        except: pass
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Init DB Error: {e}")

def get_count_workload():
    conn = get_db_connection()
    if not conn: return 0
    try:
        cur = conn.cursor()
        sql = "SELECT count(*) FROM pmu_measurements WHERE time > NOW() - INTERVAL '10 seconds';"
        cur.execute(sql)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0] if result else 0
    except Exception as e:
        return 0

def save_metrics(cpu, mem):
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        sql = """
            INSERT INTO system_metrics (time, service_name, cpu_percent, memory_mb)
            VALUES (NOW(), %s, %s, %s)
        """
        cur.execute(sql, (SERVICE_NAME, cpu, mem))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Save Metrics Error] {e}")

def burn_cpu(row_count):
    if row_count <= 0:
        return
    
    base_loops = 100000 
    iterations = row_count * base_loops
    
    print(f"Load: {row_count} records -> Processing...")
    
    x = 1.0001
    for _ in range(iterations):
        x = x * x
        if x > 1e200: x = 1.0001
        
time.sleep(5)
init_metrics_db()

current_process = psutil.Process(os.getpid())
current_process.cpu_percent()

while True:
    try:
      
        current_count = get_count_workload()
        burn_cpu(current_count)
        
        cpu_usage = current_process.cpu_percent(interval=None) / psutil.cpu_count()
        
        memory_usage = current_process.memory_info().rss / 1024 / 1024
        
        save_metrics(cpu_usage, memory_usage)
        print(f"Metrics Saved: CPU={cpu_usage:.2f}%, RAM={memory_usage:.2f}MB")
        
        time.sleep(10)

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)