package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	nbql "github.com/INLOpen/nexusbase/clients/nbql/golang"
)

const (
	numWriters      = 5  // จำนวน Goroutine ที่ทำหน้าที่เขียนข้อมูล
	numReaders      = 2  // จำนวน Goroutine ที่ทำหน้าที่อ่านข้อมูล
	writesPerWriter = 100 // จำนวนข้อมูลที่แต่ละ Writer จะส่ง
	host            = "10.1.1.1:50052"
)

func main() {
	// --- 1. เชื่อมต่อกับเซิร์ฟเวอร์เพียงครั้งเดียว ---
	// เราจะแชร์ client instance ตัวนี้ให้กับทุก Goroutine
	// Client ถูกออกแบบมาให้ปลอดภัยสำหรับการใช้งานพร้อมกัน (thread-safe)
	opts := nbql.Options{Address: host}
	client, err := nbql.Connect(context.Background(), opts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()
	fmt.Printf("✅ Connected to server. Starting %d writers and %d readers.\n", numWriters, numReaders)

	var wg sync.WaitGroup

	// --- 2. เริ่มการทำงานของ Writer goroutines ---
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go writerWorker(i, &wg, client)
	}

	// --- 3. เริ่มการทำงานของ Reader goroutines ---
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go readerWorker(i, &wg, client)
	}

	// --- 4. รอให้ Worker ทั้งหมดทำงานจนเสร็จ ---
	fmt.Println("Waiting for all workers to complete...")
	wg.Wait()
	fmt.Println("✅ All workers finished successfully.")
}

// writerWorker จำลอง service ที่ทำหน้าที่เขียนข้อมูลอย่างต่อเนื่อง
func writerWorker(id int, wg *sync.WaitGroup, client *nbql.Client) {
	defer wg.Done()
	log.Printf("[Writer %d] Starting...\n", id)

	for i := 0; i < writesPerWriter; i++ {
		metric := "concurrent.test.data"
		tags := map[string]string{"writer_id": fmt.Sprintf("%d", id)}
		fields := map[string]interface{}{"value": rand.Float64() * 100, "iteration": int64(i)}
		timestamp := time.Now().UnixNano()

		err := client.Push(context.Background(), metric, tags, fields, timestamp)
		if err != nil {
			log.Printf("[Writer %d] ❌ Push failed: %v\n", id, err)
		} else if i%20 == 0 { // Log ทุกๆ 20 ครั้งเพื่อไม่ให้รก console
			log.Printf("[Writer %d] Pushed data point #%d\n", id, i)
		}
		time.Sleep(time.Duration(10+rand.Intn(40)) * time.Millisecond) // จำลองการทำงานอื่นๆ
	}
	log.Printf("[Writer %d] Finished.\n", id)
}

// readerWorker จำลอง service ที่ทำหน้าที่ Query ข้อมูลเป็นระยะ
func readerWorker(id int, wg *sync.WaitGroup, client *nbql.Client) {
	defer wg.Done()
	log.Printf("[Reader %d] Starting...\n", id)

	// Worker นี้จะ Query ทั้งหมด 5 ครั้ง โดยมี delay ระหว่างแต่ละครั้ง
	for i := 0; i < 5; i++ {
		startTime := time.Now().Add(-10 * time.Second).UnixNano()
		endTime := time.Now().UnixNano()
		metricName := "concurrent.test.data"
		queryTemplate := `QUERY ? FROM ? TO ?;`

		// Using parameterized queries is thread-safe and prevents injection.
		result, err := client.Query(context.Background(), queryTemplate, metricName, startTime, endTime)
		if err != nil {
			log.Printf("[Reader %d] ❌ Query failed: %v\n", id, err)
		} else {
			log.Printf("[Reader %d] ✅ Query #%d successful, received %d rows.\n", id, i+1, result.TotalRows)
		}
		time.Sleep(1 * time.Second) // รอสักครู่ก่อน Query ครั้งถัดไป
	}
	log.Printf("[Reader %d] Finished.\n", id)
}
