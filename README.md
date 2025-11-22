# NexusBase: A High-Performance Time Series Database

**Version 0.0.1**

**⚠️ คำเตือน: โปรเจกต์นี้กำลังอยู่ขั้นตอนการพัฒนา และยังไม่พร้อมสำหรับการใช้งานในระดับ Production ⚠️**

---

**NexusBase** คือโปรเจกต์ฐานข้อมูลอนุกรมเวลา (Time Series Database - TSDB) ประสิทธิภาพสูงที่สร้างขึ้นด้วยภาษา Go โดยถูกออกแบบมาเพื่อรองรับปริมาณการเขียนข้อมูลจำนวนมาก (High Write Throughput) และการค้นคืนข้อมูลที่มีประสิทธิภาพ โดยมีหัวใจหลักคือสถาปัตยกรรมแบบ Log-Structured Merge-tree (LSM-tree)

## ภาพรวมสถาปัตยกรรม (Architecture Overview)

วงจรชีวิตของข้อมูลใน NexusBase เริ่มต้นจากการรับคำขอ, ผ่านเส้นทางการเขียน (Write Path) ที่รวดเร็ว, จัดเก็บลงดิสก์อย่างมีแบบแผน, และถูกค้นคืนผ่านเส้นทางการอ่าน (Read Path) ที่มีประสิทธิภาพ

```
[Client] -> gRPC Server -> [Write Path] -> [Read Path] -> [Client]
                               |                ^
                               |                |
                               V                |
                        +--------------+        |
                        |     WAL      |        |
                        +--------------+        |
                               |                |
                               V                |
                        +--------------+   +-------------------+
                        |   Memtable   |-->| Immutable         |
                        | (In-Memory)  |   | Memtables (Queue) |
                        +--------------+   +-------------------+
                               |                |
                               | (Flush)        |
                               V                |
                        +--------------+        |
                        | SSTable (L0) |        |
                        +--------------+        |
                               |                |
                               V (Compaction)   |
      +----------------------------------------------------------+
      | SSTables (L1, L2, ...) on Disk (Managed by LevelsManager)|
      +----------------------------------------------------------+
```

## คุณสมบัติหลัก (Features)

*   **High-Performance Ingestion:** เส้นทางการเขียนข้อมูลที่ถูกปรับให้มีประสิทธิภาพสูงสุดโดยใช้ Write-Ahead Log (WAL) และ Memtable ในหน่วยความจำ
*   **LSM-Tree Architecture:** จัดการข้อมูลบนดิสก์อย่างมีประสิทธิภาพด้วย SSTables และกระบวนการ Compaction แบบแบ่งระดับ (Level-based)
*   **Advanced Querying:** รองรับการค้นหาข้อมูลตามช่วงเวลา, การทำ Downsampling, และฟังก์ชันการรวมข้อมูล (Aggregation)
*   **Efficient Tag Indexing:** ค้นหา Series ได้อย่างรวดเร็วโดยใช้ Roaring Bitmaps สำหรับการทำดัชนีแบบกลับด้าน (Inverted Index)
*   **Durability and Recovery:** มีกลไก WAL สำหรับการกู้คืนข้อมูล และฟีเจอร์ Snapshot & Restore สำหรับการสำรองข้อมูล
*   **Real-time Subscriptions:** สามารถติดตามการเปลี่ยนแปลงของข้อมูลแบบ Real-time ผ่าน gRPC streams

## การเริ่มต้นใช้งาน (Getting Started)

### ข้อกำหนดเบื้องต้น
*   Go Toolchain (เวอร์ชัน 1.23 หรือสูงกว่า)
*   ระบบปฏิบัติการที่รองรับ POSIX (เช่น Linux, macOS)

### การติดตั้ง
1.  **โคลน Repository:**
    ```bash
    git clone https://github.com/INLOpen/nexusbase.git
    cd nexusbase
    ```
2.  **คอมไพล์โปรแกรม:**
    ```bash
    # คอมไพล์เซิร์ฟเวอร์หลัก
    go build -o tsdb-server ./cmd/server
    # คอมไพล์เครื่องมือสำหรับกู้คืนข้อมูล
    go build -o restore-util ./cmd/restore-util
    ```

### การรันเซิร์ฟเวอร์
1.  กำหนดค่าเซิร์ฟเวอร์โดยแก้ไขไฟล์ `cmd/server/config.yaml`
2.  เริ่มการทำงานของเซิร์ฟเวอร์:
    ```bash
    ./tsdb-server -config /path/to/your/config.yaml
    ```
### เลือก Engine (Engine Mode)

NexusBase มีสองการใช้งานของ engine ในโค้ด: the legacy `StorageEngine` (เรียกว่า "engine") และ lightweight `Engine2` (เรียกว่า "engine2").
ค่าคอนฟิกใหม่ `engine.mode` ควบคุมว่าส่วนใดของ engine จะถูกสตาร์ทเมื่อรันเซิร์ฟเวอร์:

- `engine` : รัน `StorageEngine` แบบดั้งเดิม (LSM-based).
- `engine2` (recommended default): รัน `Engine2` แบบ lightweight ซึ่งในเวอร์ชันนี้ถูกตั้งให้เป็นค่าเริ่มต้นของระบบ — ใช้เมื่อคุณต้องการให้ Engine2 ทำหน้าที่เป็น storage engine หลัก
- `both`: รัน `StorageEngine` แบบดั้งเดิมเป็น engine หลัก และยังสตาร์ท `Engine2` (เชื่อมต่อกับ `HookManager` ของ engine หลัก) เพื่อความเข้ากันได้หรือการย้ายข้อมูลแบบคู่ขนาน

ตัวอย่างการตั้งค่าใน `configs/config-test-leader.yaml`:

```yaml
engine:
    mode: both
    data_dir: "./data-leader"
    # ... (อื่นๆ)
```

และตัวอย่าง follower ที่ใช้ `engine2`:

```yaml
engine:
    mode: engine2
    data_dir: "./data-follower"
    # ... (อื่นๆ)
```

แนะนำ: ถ้าคุณต้องการทดสอบฟังก์ชันใหม่หรือรันในสภาพแวดล้อมเบาๆ ให้ใช้ `engine2`. ถ้าต้องการใช้งาน LSM เต็มรูปแบบใน production ให้ใช้ `engine`.

## Documentation
WIP

## การมีส่วนร่วม (Contributing)

เรายินดีต้อนรับทุกการมีส่วนร่วม! โปรดอ่าน [แนวทางการมีส่วนร่วม (CONTRIBUTING.md)](CONTRIBUTING.md) สำหรับรายละเอียดเกี่ยวกับขั้นตอนการส่งโค้ด, การรายงานข้อผิดพลาด, และการเสนอคุณสมบัติใหม่

## ข้อจำกัดความรับผิดชอบ (Disclaimer)

แม้ว่าจะมีการนำฟีเจอร์หลักๆ ของ TSDB มาใช้งาน แต่ยังขาดการทดสอบที่ครอบคลุม, การเสริมความแข็งแกร่ง (Hardening), และเครื่องมือที่จำเป็นสำหรับสภาพแวดล้อมระดับ Production โปรดใช้งานด้วยความระมัดระวัง