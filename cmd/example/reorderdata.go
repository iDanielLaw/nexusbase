package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// --- 1. Define the Generic Constraint ---

// OrderableData interface กำหนด Contract สำหรับข้อมูลทุกชนิดที่สามารถจัดเรียงได้
type OrderableData interface {
	// GetID() Method ที่จะคืนค่า ID หรือ Timestamp สำหรับการจัดเรียง
	// ค่าที่คืนต้องเป็น int64 เพื่อให้เข้ากับ atomic.Int64 และใช้เป็น Map Key ได้
	GetID() int64
}

// --- 2. Implement the constraint for your data types ---

// WebSocketData implements OrderableData
type WebSocketData struct {
	ID  int64 `json:"id"`
	Val int   `json:"val"`
}

func (d WebSocketData) GetID() int64 {
	return d.ID
}

// TimeSeriesData implements OrderableData
type TimeSeriesData struct {
	TS    int64 `json:"ts"` // Timestamp
	Open  int   `json:"open"`
	High  int   `json:"high"`
	Low   int   `json:"low"`
	Close int   `json:"close"`
}

func (d TimeSeriesData) GetID() int64 {
	return d.TS
}

// --- 3. Make ReorderingBufferManager Generic ---

// ReorderingBufferManager จัดการ Reordering Buffer และสถานะ
// T เป็น Type Parameter ที่ต้อง Implement OrderableData interface
type ReorderingBufferManager[T OrderableData] struct {
	buffer     map[int64]T  // Key เป็น int64 (ID/TS), Value เป็น Type T
	expectedID atomic.Int64 // ID/TS ถัดไปที่คาดหวัง
	mu         sync.Mutex   // Mutex สำหรับ buffer (map)
	outputChan chan T       // Channel สำหรับส่งข้อมูลที่เรียงแล้วออกไป (Type T)
	stopChan   chan struct{}
	wg         sync.WaitGroup
	timeout    time.Duration
}

// NewReorderingBufferManager สร้าง Manager ใหม่สำหรับ Generic Type T
func NewReorderingBufferManager[T OrderableData](bufferSize int, initialExpectedID int64, timeout time.Duration) *ReorderingBufferManager[T] {
	rbm := &ReorderingBufferManager[T]{
		buffer:     make(map[int64]T),
		outputChan: make(chan T, bufferSize),
		stopChan:   make(chan struct{}),
		timeout:    timeout,
	}
	rbm.expectedID.Store(initialExpectedID) // กำหนดค่าเริ่มต้นของ expectedID
	rbm.wg.Add(1)
	go rbm.runTimeoutCheck()
	return rbm
}

// Stop สั่งให้ Manager หยุดทำงานและรอให้ Goroutine ย่อยหยุด
func (rbm *ReorderingBufferManager[T]) Stop() {
	close(rbm.stopChan)
	rbm.wg.Wait()
	close(rbm.outputChan)
	fmt.Println("Reordering Buffer Manager: หยุดทำงานแล้ว.")
}

// GetOutputChannel ส่งคืน Channel ที่ใช้รับข้อมูลที่จัดเรียงแล้ว
func (rbm *ReorderingBufferManager[T]) GetOutputChannel() <-chan T {
	return rbm.outputChan
}

// processData รับข้อมูล T และจัดการ Reordering
func (rbm *ReorderingBufferManager[T]) processData(data T) {
	currentDataID := data.GetID() // ใช้ GetID() จาก interface
	currentExpectedID := rbm.expectedID.Load()

	if currentDataID < currentExpectedID {
		fmt.Printf("[Buffer] ได้รับข้อมูลเก่า/ซ้ำ: ID %d (คาดหวัง %d), ทิ้ง.\n", currentDataID, currentExpectedID)
		return
	}

	rbm.mu.Lock()
	defer rbm.mu.Unlock()

	// Double check after acquiring lock
	if currentDataID < rbm.expectedID.Load() {
		fmt.Printf("[Buffer] ได้รับข้อมูลเก่า/ซ้ำหลังล็อก: ID %d (คาดหวัง %d), ทิ้ง.\n", currentDataID, rbm.expectedID.Load())
		return
	}

	if currentDataID == rbm.expectedID.Load() {
		fmt.Printf("[Buffer] ได้รับข้อมูลตรงลำดับ: ID %d. ส่งออก.\n", currentDataID)
		rbm.outputChan <- data
		rbm.expectedID.Add(1) // สมมติว่า ID/TS เพิ่มทีละ 1
		rbm.flushBuffer()
	} else { // currentDataID > rbm.expectedID.Load() (ข้อมูลมาข้ามลำดับ)
		if _, exists := rbm.buffer[currentDataID]; exists {
			fmt.Printf("[Buffer] ได้รับข้อมูล ID %d ซ้ำใน Buffer, อัปเดต.\n", currentDataID)
		} else {
			fmt.Printf("[Buffer] ได้รับข้อมูลข้ามลำดับ: ID %d (คาดหวัง %d). เก็บใน Buffer.\n", currentDataID, rbm.expectedID.Load())
		}
		rbm.buffer[currentDataID] = data
	}
}

// flushBuffer พยายามดึงข้อมูลจาก buffer ออกไปตามลำดับ
// ฟังก์ชันนี้ต้องถูกเรียกภายใต้ Mutex lock เสมอ
func (rbm *ReorderingBufferManager[T]) flushBuffer() {
	for {
		currentExpectedID := rbm.expectedID.Load()
		if bufferedData, ok := rbm.buffer[currentExpectedID]; ok {
			fmt.Printf("[Buffer] ดึงข้อมูลจาก Buffer: ID %d. ส่งออก.\n", currentExpectedID)
			rbm.outputChan <- bufferedData
			delete(rbm.buffer, currentExpectedID)
			rbm.expectedID.Add(1)
		} else {
			break
		}
	}
}

// runTimeoutCheck
func (rbm *ReorderingBufferManager[T]) runTimeoutCheck() {
	defer rbm.wg.Done()
	ticker := time.NewTicker(rbm.timeout)
	defer ticker.Stop()

	fmt.Println("[TimeoutChecker] เริ่มทำงาน...")

	for {
		select {
		case <-ticker.C:
			rbm.mu.Lock()

			currentExpectedID := rbm.expectedID.Load()

			if len(rbm.buffer) > 0 {
				foundNextIDInBuffer := false
				for id := range rbm.buffer { // ID in map key is int64
					if id == currentExpectedID {
						foundNextIDInBuffer = true
						break
					}
				}

				if !foundNextIDInBuffer {
					minBufferedID := int64(-1)
					for id := range rbm.buffer {
						if id > currentExpectedID {
							if minBufferedID == -1 || id < minBufferedID {
								minBufferedID = id
							}
						}
					}

					if minBufferedID != -1 {
						fmt.Printf("[TimeoutChecker] ID %d หายไปนานเกิน %v. ข้ามไปที่ ID ที่เล็กที่สุดใน Buffer (%d).\n", currentExpectedID, rbm.timeout, minBufferedID)
						rbm.expectedID.Store(minBufferedID)
						rbm.flushBuffer()
					}
				}
			}
			rbm.mu.Unlock()
		case <-rbm.stopChan:
			fmt.Println("[TimeoutChecker] ได้รับสัญญาณปิด. กำลังปิดตัว.")
			return
		}
	}
}

// --- Main Application Example ---
func main() {
	fmt.Println("--- เริ่มต้น Generic Reordering Buffer ใน Go ---")

	bufferSize := 100

	// --- 1. ใช้กับ WebSocketData ---
	fmt.Println("\n--- ทดสอบกับ WebSocketData ---")
	// สร้าง Manager สำหรับ WebSocketData
	wsManager := NewReorderingBufferManager[WebSocketData](bufferSize, 1, 500*time.Millisecond) // initialExpectedID: 1

	var wsConsumerWg sync.WaitGroup
	wsConsumerWg.Add(1)
	go func() {
		defer wsConsumerWg.Done()
		fmt.Println("[WS Consumer] เริ่มรับข้อมูล WebSocket ที่จัดเรียงแล้ว...")
		for data := range wsManager.GetOutputChannel() {
			fmt.Printf("[WS Consumer] ได้รับ: ID %d, Val %d\n", data.ID, data.Val)
		}
		fmt.Println("[WS Consumer] Channel ปิด, หยุดรับข้อมูล.")
	}()

	mockWSData := []WebSocketData{
		{ID: 1, Val: 101},
		{ID: 3, Val: 103},
		{ID: 2, Val: 102},
		{ID: 5, Val: 105},
		{ID: 4, Val: 104},
	}

	fmt.Println("[WS Sender] กำลังจำลองการส่ง WebSocket Data...")
	for _, data := range mockWSData {
		go wsManager.processData(data)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(1 * time.Second) // ให้เวลาประมวลผล
	wsManager.Stop()
	wsConsumerWg.Wait()

	// --- 2. ใช้กับ TimeSeriesData ---
	fmt.Println("\n--- ทดสอบกับ TimeSeriesData ---")
	// สร้าง Manager สำหรับ TimeSeriesData
	initialTS := int64(12837933332)
	tsManager := NewReorderingBufferManager[TimeSeriesData](bufferSize, initialTS, 500*time.Millisecond)

	var tsConsumerWg sync.WaitGroup
	tsConsumerWg.Add(1)
	go func() {
		defer tsConsumerWg.Done()
		fmt.Println("[TS Consumer] เริ่มรับ Time-Series Data ที่จัดเรียงแล้ว...")
		for data := range tsManager.GetOutputChannel() {
			fmt.Printf("[TS Consumer] ได้รับ: TS %d, Open %d, Close %d\n", data.TS, data.Open, data.Close)
		}
		fmt.Println("[TS Consumer] Channel ปิด, หยุดรับข้อมูล.")
	}()

	mockTSData := []TimeSeriesData{
		{TS: 12837933332, Open: 12, High: 19, Low: 10, Close: 11},
		{TS: 12837933334, Open: 13, High: 20, Low: 11, Close: 12}, // ข้าม 33333
		{TS: 12837933333, Open: 12, High: 19, Low: 10, Close: 11}, // มาถึงช้า
		{TS: 12837933336, Open: 15, High: 22, Low: 13, Close: 14}, // ข้าม 33335
		{TS: 12837933335, Open: 14, High: 21, Low: 12, Close: 13}, // มาถึงช้า
	}

	fmt.Println("[TS Sender] กำลังจำลองการส่ง Time-Series Data...")
	for _, data := range mockTSData {
		go tsManager.processData(data)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(1 * time.Second) // ให้เวลาประมวลผล
	tsManager.Stop()
	tsConsumerWg.Wait()

	fmt.Println("\n--- Generic Reordering Buffer การจำลองเสร็จสิ้น ---")
}
