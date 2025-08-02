package main

import (
	"fmt"
	"math"
)

// กำหนดค่าคงที่ (weight) สำหรับแต่ละปัจจัย
const (
	alpha1 = 1.0
	alpha2 = 1.0
	alpha3 = 1.0
	beta1  = 1.0
	beta2  = 1.0
	beta3  = 1.0
	wWA    = 0.5 // น้ำหนักของ Write Amplification
	wRA    = 0.5 // น้ำหนักของ Read Amplification
)

// ฟังก์ชันคำนวณ Write Amplification
// WA ประมาณค่าจาก:
// - (C/S): อัตราการเขียนต่อขนาด SSTable -> ยิ่งเขียนเยอะ/ไฟล์เล็ก ยิ่ง Flush บ่อย WA สูง
// - (1/M): จำนวนไฟล์ใน L0 -> ยิ่ง M น้อย ยิ่ง Compaction บ่อย WA สูง
// - (1/T): ความถี่ในการ Compaction -> ยิ่ง T น้อย (ทำบ่อย) WA สูง
func calcWA(C, S, M, T float64) float64 {
	return alpha1*(C/S) + alpha2*(1/M) + alpha3*(1/T)
}

// ฟังก์ชันคำนวณ Read Amplification
// RA ประมาณค่าจาก:
// - M: จำนวนไฟล์ใน L0 -> ยิ่ง M เยอะ ยิ่งต้องค้นหาหลายไฟล์ RA สูง
// - (C/S): อัตราการเขียนต่อขนาด SSTable -> ยิ่งสูง หมายถึงมีข้อมูลกระจายหลายไฟล์ RA สูง
// - T: ความถี่ในการ Compaction -> ยิ่ง T มาก (ทำไม่บ่อย) ไฟล์ใน L0 ยิ่งสะสมเยอะ RA สูง
func calcRA(C, S, M, T float64) float64 {
	return beta1*M + beta2*(C/S) + beta3*T
}

// ฟังก์ชันเป้าหมายสำหรับหาค่าที่เหมาะสม
func objectiveFunc(C, S, M, T float64) float64 {
	WA := calcWA(C, S, M, T)
	RA := calcRA(C, S, M, T)
	return wWA*WA + wRA*RA
}

func main() {
	C := 100.0 // write throughput records/sec

	// ช่วงค่าที่ต้องการทดสอบ
	maxL0FilesList := []float64{4, 6, 8, 10, 12, 16, 24, 32, 36, 40, 48, 56, 64}
	sstableSizeList := []float64{1, 4, 8, 16, 32, 64}       // MB
	compIntervalList := []float64{10, 30, 60, 90, 120, 300} // Sec

	bestScore := math.MaxFloat64
	var bestM, bestS, bestT float64

	for _, M := range maxL0FilesList {
		for _, S := range sstableSizeList {
			for _, T := range compIntervalList {
				score := objectiveFunc(C, S, M, T)
				if score < bestScore {
					bestScore = score
					bestM = M
					bestS = S
					bestT = T
				}
			}
		}
	}

	fmt.Println("Best Parameters:")
	fmt.Printf("  MaxL0Files = %.0f\n", bestM)
	fmt.Printf("  TargetSSTableSize = %.0f MB\n", bestS)
	fmt.Printf("  CompactionIntervalSeconds = %.0f\n", bestT)
	fmt.Printf("  Objective Score = %.4f (ยิ่งต่ำยิ่งดี)\n", bestScore)
}
