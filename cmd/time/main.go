package main

import (
	"fmt"
	"time"
)

func main() {
	// 1. โหลด Time Zone ของประเทศไทย
	// โดยปกติประเทศไทยจะใช้ Asia/Bangkok
	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		fmt.Println("Error loading location:", err)
		return
	}

	// 2. ใช้ time.Now() เพื่อดึงเวลาปัจจุบัน (UTC หรือ Time Zone ของเครื่อง)
	now := time.Now()

	// 3. แปลงเวลาปัจจุบันให้เป็น Time Zone ของประเทศไทย
	thaiTime := now.In(loc).Add(-1 * time.Minute)

	fmt.Println("เวลาปัจจุบัน (UTC หรือ Time Zone ของเครื่อง):", now)
	fmt.Println("เวลาปัจจุบันในประเทศไทย (Asia/Bangkok):", thaiTime)
	fmt.Println("Time Zone ของประเทศไทย:", thaiTime.Location())
	fmt.Println("UnixNano:", thaiTime.UnixNano())
}
