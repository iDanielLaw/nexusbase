# NexusBase Replication Testing

คู่มือสำหรับทดสอบ Replication ใน NexusBase

## 📁 โครงสร้างไฟล์

```
nexusbase/
├── configs/                          # Configuration files
│   ├── config-test-leader.yaml       # Leader config (local testing)
│   ├── config-test-follower.yaml     # Follower config (local testing)
│   ├── config-docker-leader.yaml     # Leader config (Docker)
│   └── config-docker-follower.yaml   # Follower config (Docker)
├── testing/                          # Testing scripts and tools
│   ├── quick-start-replication.ps1   # Quick start script for local testing
│   ├── test-replication.ps1          # Automated local test
│   ├── docker-compose-replication.yaml  # Docker Compose configuration
│   └── test-docker-replication.ps1   # Automated Docker test
└── docs/                             # Documentation
    ├── QUICK-START-REPLICATION.md    # Local testing guide
    └── DOCKER-REPLICATION.md         # Docker testing guide
```

## 🚀 Quick Start

### วิธีที่ 1: ทดสอบแบบ Local (Windows)

**เริ่มต้นง่ายที่สุด:**
```powershell
# Terminal 1: เริ่ม Leader
.\testing\quick-start-replication.ps1 -Mode leader

# Terminal 2: เริ่ม Follower
.\testing\quick-start-replication.ps1 -Mode follower

# Terminal 3: ทดสอบ Replication
.\testing\test-replication.ps1
```

**ดูคู่มือเต็ม:** [docs/QUICK-START-REPLICATION.md](docs/QUICK-START-REPLICATION.md)

### วิธีที่ 2: ทดสอบด้วย Docker

**เริ่มต้นง่ายที่สุด:**
```powershell
# ทดสอบทันที (อัตโนมัติทั้งหมด)
.\testing\test-docker-replication.ps1 -CleanStart

# หรือเริ่ม services manually
cd testing
docker-compose -f docker-compose-replication.yaml up -d
```

**ดูคู่มือเต็ม:** [docs/DOCKER-REPLICATION.md](docs/DOCKER-REPLICATION.md)

## 📚 เอกสาร

- **[QUICK-START-REPLICATION.md](docs/QUICK-START-REPLICATION.md)** - คู่มือการทดสอบแบบ Local
  - วิธีใช้ quick-start script
  - การทดสอบด้วย grpcurl
  - Troubleshooting
  - ตัวอย่างคำสั่งต่างๆ

- **[DOCKER-REPLICATION.md](docs/DOCKER-REPLICATION.md)** - คู่มือการทดสอบด้วย Docker
  - Docker Compose configuration
  - Health checks และ monitoring
  - Failover testing
  - Container management

## 🔧 Configuration Files

### Local Testing (configs/)

**config-test-leader.yaml:**
- Ports: gRPC=50051, TCP=50052, Replication=50053
- Data: `./data-leader`
- Mode: leader

**config-test-follower.yaml:**
- Ports: gRPC=50055, TCP=50056
- Data: `./data-follower`
- Mode: follower
- Connects to: `localhost:50053`

### Docker Testing (configs/)

**config-docker-leader.yaml:**
- Same ports as local
- Data: `/app/data` (Docker volume)
- Listen: `0.0.0.0` (accessible from all networks)

**config-docker-follower.yaml:**
- Same ports as local
- Data: `/app/data` (Docker volume)
- Connects to: `nexusbase-leader:50053` (Docker network)

## 🧪 Testing Scripts

### Local Testing

**quick-start-replication.ps1** - เริ่มต้น Leader/Follower
```powershell
# Build
.\testing\quick-start-replication.ps1 -Mode build

# Clean data
.\testing\quick-start-replication.ps1 -Mode clean

# Start Leader
.\testing\quick-start-replication.ps1 -Mode leader

# Start Follower
.\testing\quick-start-replication.ps1 -Mode follower

# With TLS
.\testing\quick-start-replication.ps1 -Mode leader -WithTLS
```

**test-replication.ps1** - ทดสอบอัตโนมัติ
```powershell
# ทดสอบ 10 points
.\testing\test-replication.ps1

# ทดสอบ 100 points
.\testing\test-replication.ps1 -NumPoints 100
```

### Docker Testing

**docker-compose-replication.yaml** - Docker Compose config
```powershell
cd testing

# Start
docker-compose -f docker-compose-replication.yaml up -d

# View logs
docker-compose -f docker-compose-replication.yaml logs -f

# Stop
docker-compose -f docker-compose-replication.yaml down -v
```

**test-docker-replication.ps1** - ทดสอบอัตโนมัติ
```powershell
# Clean start และทดสอบ
.\testing\test-docker-replication.ps1 -CleanStart

# ทดสอบด้วยข้อมูลเยอะขึ้น
.\testing\test-docker-replication.ps1 -CleanStart -NumPoints 100
```

## 📊 Monitoring และ Debugging

### Local

```powershell
# Leader metrics
curl http://localhost:6060/debug/vars

# Follower metrics
curl http://localhost:6061/debug/vars

# Query UI
# Leader:   http://localhost:8088/query
# Follower: http://localhost:8089/query
```

### Docker

```powershell
# Container logs
docker-compose -f testing/docker-compose-replication.yaml logs -f

# Container stats
docker stats nexusbase-leader nexusbase-follower

# Exec into container
docker exec -it nexusbase-leader sh
docker exec -it nexusbase-follower sh
```

## 🧹 Cleanup

### Local
```powershell
# Clean data directories
.\testing\quick-start-replication.ps1 -Mode clean

# หรือ manual
Remove-Item -Recurse -Force data-leader, data-follower
```

### Docker
```powershell
# Stop และลบ containers + volumes
cd testing
docker-compose -f docker-compose-replication.yaml down -v

# ลบ images ด้วย
docker-compose -f docker-compose-replication.yaml down -v --rmi all
```

## ⚡ คำสั่งที่ใช้บ่อย

```powershell
# Local testing - Quick test
.\testing\quick-start-replication.ps1 -Mode clean
# Terminal 1: .\testing\quick-start-replication.ps1 -Mode leader
# Terminal 2: .\testing\quick-start-replication.ps1 -Mode follower
# Terminal 3: .\testing\test-replication.ps1

# Docker testing - Quick test
.\testing\test-docker-replication.ps1 -CleanStart

# ดู logs real-time
# Local: ดูใน terminal ที่รัน leader/follower
# Docker: docker-compose -f testing/docker-compose-replication.yaml logs -f
```

## 📖 อ่านเพิ่มเติม

- **Architecture**: [docs/architecture.md](docs/architecture.md)
- **Replication Design**: [docs/replicated-wal.md](docs/replicated-wal.md)
- **TLS Setup**: [docs/tls-setup-guide.md](docs/tls-setup-guide.md)
- **Admin Guide**: [docs/admin-guide.md](docs/admin-guide.md)

## ❓ Troubleshooting

ดูคู่มือ Troubleshooting ใน:
- [docs/QUICK-START-REPLICATION.md#troubleshooting](docs/QUICK-START-REPLICATION.md#troubleshooting)
- [docs/DOCKER-REPLICATION.md#troubleshooting](docs/DOCKER-REPLICATION.md#troubleshooting)

---

**Happy Testing! 🚀**
