# Docker Compose - NexusBase Replication Testing

วิธีใช้ Docker Compose เพื่อทดสอบ NexusBase Replication แบบ Leader-Follower

## 📋 ไฟล์ที่เกี่ยวข้อง

- `docker-compose-replication.yaml` - Docker Compose configuration
- `config-docker-leader.yaml` - Leader node configuration
- `config-docker-follower.yaml` - Follower node configuration
- `Dockerfile` - Docker image build file

## 🚀 เริ่มต้นใช้งาน

### 1. Build และเริ่ม Services

```powershell
# Build และเริ่ม Leader และ Follower
docker-compose -f docker-compose-replication.yaml up --build

# หรือรันแบบ background
docker-compose -f docker-compose-replication.yaml up -d --build
```

### 2. ตรวจสอบสถานะ

```powershell
# ดูสถานะ containers
docker-compose -f docker-compose-replication.yaml ps

# ดู logs
docker-compose -f docker-compose-replication.yaml logs -f

# ดู logs เฉพาะ Leader
docker-compose -f docker-compose-replication.yaml logs -f nexusbase-leader

# ดู logs เฉพาะ Follower
docker-compose -f docker-compose-replication.yaml logs -f nexusbase-follower
```

### 3. ทดสอบการส่งข้อมูล

```powershell
# ติดตั้ง grpcurl (ครั้งเดียว)
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# ส่งข้อมูลไปที่ Leader
grpcurl -plaintext -d '{
  "metric": "cpu.usage",
  "tags": {"host": "server1", "region": "us-east"},
  "timestamp": 1699401600,
  "fields": {"value": 75.5}
}' localhost:50051 tsdb.TSDBService.PutEvent

# Query จาก Leader
grpcurl -plaintext -d '{
  "metric": "cpu.usage",
  "tags": {},
  "start_time": 1699401000,
  "end_time": 1699402000
}' localhost:50051 tsdb.TSDBService.Query

# Query จาก Follower (ควรได้ข้อมูลเดียวกัน)
grpcurl -plaintext -d '{
  "metric": "cpu.usage",
  "tags": {},
  "start_time": 1699401000,
  "end_time": 1699402000
}' localhost:50055 tsdb.TSDBService.Query
```

### 4. ตรวจสอบ Replication

```powershell
# เข้าไปใน Leader container
docker exec -it nexusbase-leader sh

# ดูข้อมูลใน Leader
ls -la /app/data

# เข้าไปใน Follower container
docker exec -it nexusbase-follower sh

# ดูข้อมูลใน Follower (ควรเหมือน Leader)
ls -la /app/data
```

### 5. ทดสอบ Health Check

```powershell
# ตรวจสอบ Leader health
curl http://localhost:6060/debug/vars

# ตรวจสอบ Follower health
curl http://localhost:6061/debug/vars
```

## 🎯 Endpoints

### Leader Node
- **gRPC**: `localhost:50051` - รับข้อมูลจาก clients
- **TCP**: `localhost:50052` - TCP binary protocol
- **Replication**: `localhost:50053` - ให้บริการ replication
- **Query UI**: `http://localhost:8088/query` - NBQL Query UI
- **Metrics**: `http://localhost:6060/debug/vars` - Metrics endpoint
- **pprof**: `http://localhost:6060/debug/pprof` - Profiling

### Follower Node
- **gRPC**: `localhost:50055` - Query endpoint (read-only)
- **TCP**: `localhost:50056` - TCP endpoint
- **Query UI**: `http://localhost:8089/query` - NBQL Query UI
- **Metrics**: `http://localhost:6061/debug/vars` - Metrics endpoint
- **pprof**: `http://localhost:6061/debug/pprof` - Profiling

## 🔄 การทดสอบ Failover

### ทดสอบหยุด Leader

```powershell
# หยุด Leader
docker-compose -f docker-compose-replication.yaml stop nexusbase-leader

# ตรวจสอบ Follower logs (จะเห็น connection error)
docker-compose -f docker-compose-replication.yaml logs -f nexusbase-follower

# เริ่ม Leader ใหม่
docker-compose -f docker-compose-replication.yaml start nexusbase-leader

# Follower ควร reconnect อัตโนมัติ
```

### ทดสอบหยุด Follower

```powershell
# หยุด Follower
docker-compose -f docker-compose-replication.yaml stop nexusbase-follower

# ส่งข้อมูลไปที่ Leader (ยังใช้งานได้ปกติ)
grpcurl -plaintext -d '{
  "metric": "test.metric",
  "tags": {"test": "true"},
  "timestamp": 1699401700,
  "fields": {"value": 100}
}' localhost:50051 tsdb.TSDBService.PutEvent

# เริ่ม Follower ใหม่
docker-compose -f docker-compose-replication.yaml start nexusbase-follower

# Follower จะ sync ข้อมูลที่พลาดไป
```

## 📊 Monitoring

### ดู Metrics แบบ Real-time

```powershell
# Leader metrics
while ($true) { curl -s http://localhost:6060/debug/vars | jq '.memstats.Alloc'; Start-Sleep -Seconds 2 }

# Follower metrics
while ($true) { curl -s http://localhost:6061/debug/vars | jq '.memstats.Alloc'; Start-Sleep -Seconds 2 }
```

### ดู Replication Lag

```powershell
# ตรวจสอบ sequence number จาก Leader logs
docker-compose -f docker-compose-replication.yaml logs nexusbase-leader | Select-String "sequence"

# เปรียบเทียบกับ Follower
docker-compose -f docker-compose-replication.yaml logs nexusbase-follower | Select-String "sequence"
```

## 🧪 ทดสอบแบบอัตโนมัติ

สร้างไฟล์ `test-docker-replication.ps1`:

```powershell
# เริ่ม services
docker-compose -f docker-compose-replication.yaml up -d

# รอให้ services พร้อม
Start-Sleep -Seconds 10

# ส่งข้อมูลไปที่ Leader
Write-Host "Sending data to Leader..." -ForegroundColor Cyan
$timestamp = [int][double]::Parse((Get-Date -UFormat %s))
grpcurl -plaintext -d "{
  `"metric`": `"test.replication`",
  `"tags`": {`"docker`": `"true`"},
  `"timestamp`": $timestamp,
  `"fields`": {`"value`": 42}
}" localhost:50051 tsdb.TSDBService.PutEvent

# รอ replication
Start-Sleep -Seconds 2

# Query จาก Follower
Write-Host "Querying from Follower..." -ForegroundColor Cyan
$result = grpcurl -plaintext -d "{
  `"metric`": `"test.replication`",
  `"tags`": {},
  `"start_time`": $($timestamp - 10),
  `"end_time`": $($timestamp + 10)
}" localhost:50055 tsdb.TSDBService.Query

if ($result -match "42") {
    Write-Host "✓ REPLICATION SUCCESSFUL!" -ForegroundColor Green
} else {
    Write-Host "✗ REPLICATION FAILED!" -ForegroundColor Red
}
```

รัน:
```powershell
.\test-docker-replication.ps1
```

## 🧹 ทำความสะอาด

```powershell
# หยุดและลบ containers
docker-compose -f docker-compose-replication.yaml down

# หยุดและลบ containers + volumes (ลบข้อมูลทั้งหมด)
docker-compose -f docker-compose-replication.yaml down -v

# ลบ images
docker-compose -f docker-compose-replication.yaml down --rmi all -v
```

## 📝 Notes

### Network Configuration
- Services ใช้ network `nexusbase-net` ร่วมกัน
- Leader และ Follower สามารถติดต่อกันผ่าน hostname: `nexusbase-leader`, `nexusbase-follower`

### Data Persistence
- ข้อมูลถูกเก็บใน Docker volumes: `leader-data` และ `follower-data`
- ข้อมูลจะไม่สูญหายเมื่อ restart containers
- ใช้ `down -v` เพื่อลบข้อมูลทั้งหมด

### Health Checks
- Docker จะตรวจสอบ health ของ containers อัตโนมัติ
- Follower จะเริ่มต้นหลังจาก Leader พร้อมแล้ว (`depends_on` with health check)

## ⚠️ Troubleshooting

### Problem: Follower ไม่ต่อ Leader ได้

**Solution:**
```powershell
# ตรวจสอบ network
docker network inspect nexusbase-replication_nexusbase-net

# ตรวจสอบว่า Leader ทำงานอยู่
docker-compose -f docker-compose-replication.yaml ps nexusbase-leader

# ตรวจสอบ Leader logs
docker-compose -f docker-compose-replication.yaml logs nexusbase-leader
```

### Problem: Build ล้มเหลว

**Solution:**
```powershell
# Clean build
docker-compose -f docker-compose-replication.yaml build --no-cache

# ตรวจสอบ Go version
go version  # ต้อง 1.23 หรือสูงกว่า
```

### Problem: Port ถูกใช้งานอยู่

**Solution:**
```powershell
# ตรวจสอบ port ที่ใช้งานอยู่
netstat -an | findstr "50051 50052 50053 50055 50056"

# เปลี่ยน port ใน docker-compose-replication.yaml ถ้าจำเป็น
```

---

**Happy Testing! 🐳**
