# Stage 1: Dependencies
# Stage นี้มีหน้าที่ดาวน์โหลด Go modules ทั้งหมด
# การแยก stage นี้ออกมาช่วยให้ Docker cache ทำงานได้ดีขึ้น
# ถ้า go.mod และ go.sum ไม่เปลี่ยนแปลง, stage นี้จะไม่ถูกรันใหม่
FROM golang:1.23-alpine AS deps
WORKDIR /app

# Copy go.mod, go.sum และ dependency ที่ถูก replace แบบ local ก่อน run go mod download
COPY go.mod go.sum ./
COPY shared/core/golang shared/core/golang
RUN go mod download

# Stage 2: Test
# Stage นี้ใช้สำหรับรัน unit tests
# หาก test ไม่ผ่าน, Docker build จะหยุดทำงานที่นี่
FROM deps AS test
# ติดตั้ง build-base (gcc, etc.) ซึ่งจำเป็นสำหรับการใช้ CGO
# CGO จำเป็นสำหรับการเปิดใช้งาน Race Detector (-race)
RUN apk add --no-cache build-base
WORKDIR /app
COPY . .
# รัน unit tests พร้อมเปิดใช้งาน Race Detector (-race)
RUN CGO_ENABLED=1 go test -race -timeout 60s -v ./...

# Stage 3: Builder
# Stage นี้ใช้สำหรับคอมไพล์แอปพลิเคชัน
FROM deps AS builder
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.39 TARGETOS TARGETARCH
WORKDIR /app
COPY . .
# คำสั่ง COPY --from=test นี้สร้าง dependency กับ stage 'test'
# เพื่อให้แน่ใจว่า stage 'test' จะถูกรันและต้องผ่านก่อนที่จะมาถึง stage นี้
# เราคัดลอกไฟล์ที่เรารู้ว่ามีอยู่แล้ว (go.mod) เพื่อสร้าง dependency นี้
COPY --from=test /app/go.mod ./

# ดาวน์โหลด grpc-health-probe สำหรับใช้ใน HEALTHCHECK
RUN wget -qO/bin/grpc-health-probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-${TARGETOS}-${TARGETARCH} && \
    chmod +x /bin/grpc-health-probe

# คอมไพล์แอปพลิเคชัน Go
# CGO_ENABLED=0 สร้างไบนารีแบบ statically linked
# -ldflags="-w -s" ช่วยลดขนาดของไบนารี
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /tsdb-server ./cmd/server

# Stage 4: Runner
# Stage สุดท้ายสำหรับรันแอปพลิเคชัน
# ใช้ Alpine Linux เป็น base image สำหรับรันไทม์ ซึ่งมีขนาดเล็กมาก
FROM alpine:latest

# กำหนด working directory ในคอนเทนเนอร์
WORKDIR /app

# สร้าง user และ group สำหรับรันแอปพลิเคชัน (Security Best Practice)
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
 
# คัดลอก CA certificates สำหรับการเชื่อมต่อ TLS (หาก gRPC ใช้ TLS)
# สิ่งนี้สำคัญสำหรับการสื่อสารที่ปลอดภัยกับบริการภายนอกหรือไคลเอ็นต์ที่ใช้ TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# คัดลอกไบนารีและเครื่องมือที่จำเป็นจาก builder stage พร้อมกำหนดสิทธิ์
COPY --from=builder --chown=appuser:appgroup /tsdb-server .
COPY --from=builder --chown=appuser:appgroup /bin/grpc-health-probe /bin/

# คัดลอกไฟล์การตั้งค่า (config.yaml) พร้อมกำหนดสิทธิ์
COPY --chown=appuser:appgroup cmd/server/config.yaml .
 
# คัดลอก UI files สำหรับหน้า Query และ Monitoring
COPY --chown=appuser:appgroup ui/ ./ui/
 
# สร้างไดเรกทอรีสำหรับข้อมูลและกำหนดสิทธิ์ให้ user ที่สร้างขึ้น
# ใช้ VOLUME เพื่อระบุว่า /app/data เป็นที่สำหรับเก็บข้อมูลถาวร
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data
VOLUME /app/data
 
# เปลี่ยนไปใช้ user ที่ไม่มีสิทธิ์ root
USER appuser
 
# เปิดเผยพอร์ตที่ gRPC server และ TCP server รับฟัง
EXPOSE 50051
EXPOSE 50052
EXPOSE 8080
EXPOSE 6060

# เพิ่ม HEALTHCHECK เพื่อให้ Docker สามารถตรวจสอบสถานะของ gRPC server ได้
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD ["grpc-health-probe", "-addr=localhost:50051"]
 
# กำหนด entrypoint สำหรับคอนเทนเนอร์
# แยก ENTRYPOINT และ CMD เพื่อให้สามารถ override arguments ได้ง่ายขึ้น
ENTRYPOINT ["/app/tsdb-server"]
CMD ["-config=/app/config.yaml"]