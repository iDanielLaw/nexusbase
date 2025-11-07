# Configurations Directory

Configuration files สำหรับ NexusBase Replication testing

## 📁 ไฟล์ในโฟลเดอร์นี้

### Local Testing (Windows)

- **config-test-leader.yaml** - Leader configuration
  - Ports: gRPC=50051, TCP=50052, Replication=50053, Query=8088, Debug=6060
  - Data directory: `./data-leader`
  - Mode: `leader`
  - No TLS

- **config-test-follower.yaml** - Follower configuration
  - Ports: gRPC=50055, TCP=50056, Query=8089, Debug=6061
  - Data directory: `./data-follower`
  - Mode: `follower`
  - Leader address: `localhost:50053`
  - No TLS

### Docker Testing

- **config-docker-leader.yaml** - Leader configuration for Docker
  - Same ports as local
  - Data directory: `/app/data` (mounted volume)
  - Listen address: `0.0.0.0` (accessible from Docker network)
  - Mode: `leader`

- **config-docker-follower.yaml** - Follower configuration for Docker
  - Same ports as local
  - Data directory: `/app/data` (mounted volume)
  - Leader address: `nexusbase-leader:50053` (Docker service name)
  - Mode: `follower`
  - Self-monitoring reports to: `nexusbase-leader:50051`

## 🔧 การใช้งาน

### Local Testing
```powershell
# จาก root directory
.\testing\quick-start-replication.ps1 -Mode leader   # ใช้ config-test-leader.yaml
.\testing\quick-start-replication.ps1 -Mode follower # ใช้ config-test-follower.yaml
```

### Docker Testing
```powershell
# Docker Compose จะ mount config files เหล่านี้เข้า containers อัตโนมัติ
cd testing
docker-compose -f docker-compose-replication.yaml up -d
```

## 📊 Port Mapping

### Leader
- **50051** - gRPC API (client connections)
- **50052** - TCP Binary Protocol
- **50053** - Replication Service (followers connect here)
- **8088** - Query UI & HTTP API
- **6060** - Debug/Metrics/pprof

### Follower
- **50055** - gRPC API (read-only queries)
- **50056** - TCP Binary Protocol
- **8089** - Query UI & HTTP API
- **6061** - Debug/Metrics/pprof

## 🔐 TLS Configurations

สำหรับ production หรือทดสอบด้วย TLS ให้ใช้:
- `../dev/config-leader-tls.yaml`
- `../dev/config-follower-tls.yaml`

ดูวิธี setup TLS ที่: [../docs/tls-setup-guide.md](../docs/tls-setup-guide.md)

## 📝 แก้ไข Configuration

หากต้องการเปลี่ยนแปลง config:

1. แก้ไขไฟล์ `.yaml` ที่ต้องการ
2. Restart server/container
3. ตรวจสอบ logs ว่าการเปลี่ยนแปลงมีผล

**ตัวอย่าง:**
```yaml
# เปลี่ยน log level
logging:
  level: debug  # info -> debug

# เปลี่ยน data directory
engine:
  data_dir: "./custom-data-path"

# เปลี่ยน ports
server:
  grpc_port: 60051  # เปลี่ยนจาก 50051
```

## 🔍 Validation

ตรวจสอบ config ว่าถูกต้องหรือไม่:

```powershell
# ดู config ที่กำลังใช้งาน
.\bin\nexusbase.exe --config=configs/config-test-leader.yaml --validate

# หรือรันแล้วดู logs
# ถ้า config ผิด จะมี error ตอนเริ่ม server
```

## 📚 เพิ่มเติม

- [Configuration Reference](../docs/api_reference.md)
- [Testing Guide](../TESTING-REPLICATION.md)
- [Admin Guide](../docs/admin-guide.md)
