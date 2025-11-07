# Quick Start: Testing NexusBase Replication

วิธีการรัน NexusBase เพื่อทดสอบ replication อย่างรวดเร็ว

## 🚀 วิธีใช้งานแบบง่ายที่สุด

### ขั้นตอนที่ 1: Build และเริ่ม Leader

เปิด PowerShell terminal แรก:

```powershell
cd d:\go\nexusbase

# Build และเริ่ม Leader
.\quick-start-replication.ps1 -Mode leader
```

### ขั้นตอนที่ 2: เริ่ม Follower

เปิด PowerShell terminal ที่สอง:

```powershell
cd d:\go\nexusbase

# เริ่ม Follower
.\quick-start-replication.ps1 -Mode follower
```

### ขั้นตอนที่ 3: ทดสอบ Replication

เปิด PowerShell terminal ที่สาม:

```powershell
cd d:\go\nexusbase

# ทดสอบส่งข้อมูลและตรวจสอบ replication
.\test-replication.ps1 -NumPoints 10
```

ถ้าสำเร็จคุณจะเห็น:
```
✓✓✓ REPLICATION SUCCESSFUL! ✓✓✓
  All 10 points replicated correctly
```

---

## 📋 คำสั่งที่มีให้ใช้

### quick-start-replication.ps1

Script หลักสำหรับเริ่มต้น server

```powershell
# Build binary
.\quick-start-replication.ps1 -Mode build

# Clean data directories
.\quick-start-replication.ps1 -Mode clean

# Start Leader
.\quick-start-replication.ps1 -Mode leader

# Start Follower
.\quick-start-replication.ps1 -Mode follower

# Start with TLS
.\quick-start-replication.ps1 -Mode leader -WithTLS
.\quick-start-replication.ps1 -Mode follower -WithTLS
```

### test-replication.ps1

Script สำหรับทดสอบการส่งและรับข้อมูล

```powershell
# ส่งข้อมูล 10 points
.\test-replication.ps1

# ส่งข้อมูล 100 points
.\test-replication.ps1 -NumPoints 100

# กำหนด address เอง
.\test-replication.ps1 -LeaderAddress localhost:50051 -FollowerAddress localhost:50055
```

---

## 🔧 Configuration Files

### ไม่ใช้ TLS (แนะนำสำหรับการทดสอบ)
- `config-test-leader.yaml` - Leader configuration
- `config-test-follower.yaml` - Follower configuration

### ใช้ TLS
- `dev/config-leader-tls.yaml` - Leader with TLS
- `dev/config-follower-tls.yaml` - Follower with TLS

---

## 📊 Ports และ Endpoints

### Leader Node
- **gRPC Server**: `localhost:50051` - รับข้อมูลจาก clients
- **TCP Server**: `localhost:50052` - รับข้อมูลผ่าน TCP protocol
- **Replication**: `localhost:50053` - ให้บริการ replication สำหรับ followers
- **Query API**: `localhost:8088` - HTTP query endpoint
- **Debug/Metrics**: `localhost:6060` - pprof และ metrics

### Follower Node
- **gRPC Server**: `localhost:50055` - รับ queries (read-only)
- **TCP Server**: `localhost:50056` - TCP endpoint
- **Replication Client**: เชื่อมต่อไปที่ `localhost:50053`
- **Query API**: `localhost:8089` - HTTP query endpoint
- **Debug/Metrics**: `localhost:6061` - pprof และ metrics

---

## 🧪 การทดสอบแบบ Manual

### ส่งข้อมูลด้วย grpcurl

```powershell
# Install grpcurl (ครั้งเดียว)
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# ส่งข้อมูลไปที่ Leader
grpcurl -plaintext -d '{
  "metric": "cpu.temp",
  "tags": {"host": "server1", "region": "us-east"},
  "timestamp": 1699401600,
  "fields": {"value": 75.5}
}' localhost:50051 tsdb.TSDBService.PutEvent

# Query จาก Leader
grpcurl -plaintext -d '{
  "metric": "cpu.temp",
  "tags": {},
  "start_time": 1699401000,
  "end_time": 1699402000
}' localhost:50051 tsdb.TSDBService.Query

# Query จาก Follower (ควรได้ข้อมูลเดียวกัน)
grpcurl -plaintext -d '{
  "metric": "cpu.temp",
  "tags": {},
  "start_time": 1699401000,
  "end_time": 1699402000
}' localhost:50055 tsdb.TSDBService.Query
```

### ตรวจสอบ Health

```powershell
# ตรวจสอบ Leader health
grpcurl -plaintext localhost:50053 grpc.health.v1.Health/Check

# ดู metrics
curl http://localhost:6060/debug/vars | jq .

# ดู Follower metrics
curl http://localhost:6061/debug/vars | jq .
```

---

## 🔍 ตรวจสอบและ Debug

### ดู Logs

Logs จะแสดงใน terminal ที่รัน server อยู่

**สิ่งที่ควรเห็นใน Leader:**
```
INFO Replication gRPC server listening address=localhost:50053 tls_enabled=false
INFO Replication manager starting mode=leader
INFO Follower health check addr=localhost:50054 healthy=true last_seq=100
```

**สิ่งที่ควรเห็นใน Follower:**
```
INFO Starting WAL Applier leader=localhost:50053
INFO Successfully connected to leader
INFO Applied replicated entry seq=1 type=PUT_EVENT
```

### ตรวจสอบไฟล์ข้อมูล

```powershell
# ดูข้อมูลของ Leader
Get-ChildItem -Recurse data-leader

# ดูข้อมูลของ Follower
Get-ChildItem -Recurse data-follower

# ทั้งสอง directory ควรมี structure คล้ายกัน
```

---

## ⚠️ Troubleshooting

### ปัญหา: Follower ไม่ต่อ Leader ได้

```powershell
# ตรวจสอบว่า Leader ทำงานอยู่
netstat -an | findstr 50053

# ตรวจสอบว่า Leader พร้อมรับ connection
Test-NetConnection -ComputerName localhost -Port 50053
```

### ปัญหา: ข้อมูลไม่ replicate

1. ตรวจสอบ logs ของทั้ง Leader และ Follower
2. ตรวจสอบ sequence numbers ใน health check logs
3. ลอง restart Follower

### ปัญหา: Build failed

```powershell
# ตรวจสอบ Go version
go version  # ควรเป็น 1.23 หรือใหม่กว่า

# Update dependencies
go mod tidy

# Clean build
Remove-Item -Recurse -Force bin
go build -o bin/nexusbase.exe ./cmd/server
```

---

## 🧹 ทำความสะอาด

```powershell
# ลบข้อมูลทั้งหมด
.\quick-start-replication.ps1 -Mode clean

# หรือ manual
Remove-Item -Recurse -Force data-leader, data-follower
```

---

## 📚 เอกสารเพิ่มเติม

- **คู่มือการทดสอบแบบละเอียด**: `docs/replication-testing-guide.md`
- **การตั้งค่า TLS**: `docs/tls-setup-guide.md`
- **คู่มือผู้ดูแลระบบ**: `docs/admin-guide.md`
- **SRS Documentation**: `docs/srs-en.md`

---

## 🎯 Next Steps

เมื่อทดสอบ replication สำเร็จแล้ว:

1. ทดสอบกับ TLS: `.\quick-start-replication.ps1 -Mode leader -WithTLS`
2. ทดสอบ failover: หยุด Leader แล้วเริ่มใหม่
3. ทดสอบ load: ใช้ `.\test-replication.ps1 -NumPoints 1000`
4. ตั้งค่า monitoring ด้วย Prometheus/Grafana
5. Deploy ใน production environment

---

**Happy Testing! 🚀**
