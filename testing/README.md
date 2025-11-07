# Testing Directory

ไฟล์และ scripts สำหรับทดสอบ NexusBase Replication

## 📁 ไฟล์ในโฟลเดอร์นี้

### Scripts

- **quick-start-replication.ps1** - เริ่มต้น Leader/Follower แบบง่าย
  ```powershell
  .\quick-start-replication.ps1 -Mode leader
  .\quick-start-replication.ps1 -Mode follower
  ```

- **test-replication.ps1** - ทดสอบการส่งและรับข้อมูลอัตโนมัติ
  ```powershell
  .\test-replication.ps1 -NumPoints 10
  ```

- **test-docker-replication.ps1** - ทดสอบด้วย Docker แบบอัตโนมัติ
  ```powershell
  .\test-docker-replication.ps1 -CleanStart
  ```

### Docker

- **docker-compose-replication.yaml** - Docker Compose configuration
  ```powershell
  docker-compose -f docker-compose-replication.yaml up -d
  ```

## 🚀 Quick Start

### Local Testing
```powershell
# เปิด 2 terminals จาก root directory:
# Terminal 1:
.\testing\quick-start-replication.ps1 -Mode leader

# Terminal 2:
.\testing\quick-start-replication.ps1 -Mode follower

# Terminal 3 (ทดสอบ):
.\testing\test-replication.ps1
```

### Docker Testing
```powershell
# จาก root directory:
.\testing\test-docker-replication.ps1 -CleanStart

# หรือ manual:
cd testing
docker-compose -f docker-compose-replication.yaml up -d
```

## 📚 Documentation

ดูคู่มือเต็มที่:
- [../docs/QUICK-START-REPLICATION.md](../docs/QUICK-START-REPLICATION.md) - Local testing
- [../docs/DOCKER-REPLICATION.md](../docs/DOCKER-REPLICATION.md) - Docker testing
- [../TESTING-REPLICATION.md](../TESTING-REPLICATION.md) - Overview

## 🔧 Configuration

Configuration files อยู่ใน `../configs/`:
- `config-test-leader.yaml` - Local leader
- `config-test-follower.yaml` - Local follower
- `config-docker-leader.yaml` - Docker leader
- `config-docker-follower.yaml` - Docker follower
