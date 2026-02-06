from synchrophasor.pdc import Pdc
from synchrophasor.frame import DataFrame
import os
import time
import psycopg2
from datetime import datetime




if __name__ == "__main__":
    target_ip = os.getenv("PMU_IP", "pmu-service")
    target_port = 1410
    pdc = Pdc(pdc_id=7, pmu_ip=target_ip, pmu_port=target_port)

    while True: # ลูปใหญ่สุด: กันตายถาวร
        try:
            # 1. รอจนกว่าจะต่อ PMU ติด (กันปัญหา DNS / Network Down)
            while True:
                try:
                    pdc.run()
                    print("Connected to PMU successfully!", flush=True)
                    break
                except Exception as e:
                    print(f"Waiting for PMU ({target_ip}): {e}", flush=True)
                    time.sleep(5)

            # 2. ช่วงเจรจา (Setup Phase) - จุดที่คุณพังบ่อยๆ
            # ต้องครอบ try อีกชั้น เพราะเน็ตอาจหลุดหลังต่อติดทันที
            try:
                pdc.get_header()
                pdc.get_config()
                pdc.start()
                print("Start Streaming...", flush=True)
            except (BrokenPipeError, ConnectionResetError) as e:
                print(f"Connection lost during setup: {e}. Restarting...", flush=True)
                continue # กลับไปเริ่มต่อใหม่ที่ข้อ 1

            # 3. ลูปรับข้อมูลหลัก
            while True:
                data = pdc.get()
                if not data:
                    time.sleep(0.1)
                    continue
                
                measurements = data.get_measurements()
                print(measurements, flush=True)

        except Exception as e:
            print(f"PDC Main Loop Error: {e}", flush=True)
            print("Restarting entire PDC logic in 5s...")
            time.sleep(5)