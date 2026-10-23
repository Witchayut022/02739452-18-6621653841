# ราคา SSD / RAM จากหลายร้านในไทย (02739452 Final Exam)

**หมวด:** ราคาอุปกรณ์คอมพิวเตอร์
**ขอบเขต:** SSD (M.2 NVMe / SATA) และ RAM (DDR4 / DDR5) สำหรับเดสก์ท็อป ในไทย

## แหล่งข้อมูล
| แหล่ง | วิธีดึง |
|---|---|
| ihavecpu.com | web scraping (requests) อ่าน JSON ใน `__NEXT_DATA__` ของหมวด `storage`, `ram` |
| jib.co.th | web scraping (requests + BeautifulSoup) หน้า `product_list` (SSD M.2, SSD 2.5", RAM) |
| notebookspec.com | web scraping จากหน้าสินค้าตาม sitemap (หน่วง 3 วินาทีตาม Crawl-delay) |

ไม่ใช้ Selenium, ไม่เรียก path ที่ robots.txt ห้าม (`/api`, `/jibapi`, `/go-to-shop/`), หน่วงเวลาระหว่างคำขอ, เก็บเฉพาะชื่อสินค้า/ราคา (ไม่มีข้อมูลส่วนบุคคล)

## Pipeline (Airflow DAG `pc_parts_pipeline`)
`init_db` → TaskGroup `extract` (3 แหล่งรันขนาน) → `merge_transform` (`trigger_rule=all_done`) → `quality_check` (Branch) → `load_postgres` (UPSERT) หรือ `quarantine` → `done`

- ระดับ 1: extract → transform → load → PostgreSQL → เว็บ (ตาราง + กราฟ)
- ระดับ 2: 3 แหล่งขนานด้วย TaskGroup, schema เดียวกัน, UPSERT (PK = `shop, dedup_key`) รันซ้ำแล้วจำนวนแถวไม่เพิ่ม, เว็บมี KPI และตัวกรอง
- ระดับ 3: แหล่งใดล้ม (task fail) merge ยังรวมแหล่งที่เหลือ, ตัดรายการซ้ำข้ามแหล่ง (ร้านเดียวกัน+สินค้าเดียวกัน), ตรวจคุณภาพ (จำนวนแถว, ราคา>0, ชื่อไม่ว่าง, มีครบ 2 หมวด) แล้วแตกเส้นทาง load / quarantine
- โบนัส: หน้า `/status` แสดงสถานะแต่ละแหล่งและผล data quality

## วิธีรัน
```bash
cp .env.example .env        # แก้รหัสผ่านตามต้องการ
docker compose up -d --build
```
- Airflow: http://localhost:8080 (user/password ตาม `.env`) → เปิด DAG `pc_parts_pipeline` แล้วกด Trigger (ใช้เวลาราว 4–5 นาที)
- Dashboard: http://localhost:5000 (สถานะ pipeline: http://localhost:5000/status)
- PostgreSQL: localhost:5432 ฐานข้อมูล `pcparts` ตาราง `prices`, `pipeline_runs`, `dq_reports`

ตั้งค่าได้ใน `.env`: `DQ_MIN_ROWS` (จำนวนแถวขั้นต่ำของ data quality), `NSPEC_PAGES_PER_CATEGORY`

## โครงสร้าง
```
dags/   pc_parts_pipeline.py  scrapers.py  transform.py  db.py
web/    app.py  templates/  Dockerfile
docker-compose.yml  Dockerfile.airflow  requirements.txt  .env.example
```
