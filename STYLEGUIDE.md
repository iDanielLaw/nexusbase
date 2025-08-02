# NexusBase Go Style Guide

เอกสารนี้สรุปแนวทางปฏิบัติและรูปแบบการเขียนโค้ด (Coding Style) สำหรับโปรเจกต์ NexusBase โดยอ้างอิงจากแนวทางปฏิบัติที่ดีที่สุดของภาษา Go และ [Google's Go Style Decisions](https://google.github.io/styleguide/go/) เพื่อให้โค้ดมีความสอดคล้องกัน, อ่านง่าย, และบำรุงรักษาได้สะดวก

## 1. หลักการทั่วไป (General Principles)

*   **ความเรียบง่าย (Simplicity):** เขียนโค้ดที่ตรงไปตรงมาและเข้าใจง่าย หลีกเลี่ยงความซับซ้อนที่ไม่จำเป็น
*   **ความชัดเจน (Clarity):** ชื่อตัวแปร, ฟังก์ชัน, และแพ็กเกจควรสื่อความหมายอย่างชัดเจน
*   **ความสอดคล้อง (Consistency):** ปฏิบัติตามแนวทางที่กำหนดไว้ในเอกสารนี้และรูปแบบที่มีอยู่แล้วในโค้ดเบส

## 2. การจัดรูปแบบ (Formatting)

*   **`gofmt` / `goimports`:** โค้ดทั้งหมดในโปรเจกต์จะต้องถูกจัดรูปแบบด้วย `goimports` ก่อนทำการ commit เสมอ `goimports` จะทำหน้าที่จัดรูปแบบโค้ด (เหมือน `gofmt`) และจัดกลุ่ม import โดยอัตโนมัติ
*   **การจัดกลุ่ม Imports:** `goimports` จะจัดกลุ่ม import โดยเรียงลำดับเป็น: Standard Library, Third-party, และ Project-specific
*   **ความยาวบรรทัด (Line Length):** ไม่มีการกำหนดความยาวบรรทัดที่ตายตัว แต่พยายามให้อยู่ในระดับที่อ่านง่าย (ประมาณ 80-120 ตัวอักษร)

## 3. การตั้งชื่อ (Naming Conventions)

*   **Packages:**
    *   ใช้ตัวอักษรพิมพ์เล็กทั้งหมด
    *   ตั้งชื่อให้สั้นและสื่อความหมาย (เช่น `engine`, `wal`, `sstable`)
    *   หลีกเลี่ยงการใช้ `_` (underscore) หรือ `mixedCaps`
*   **Variables:**
    *   ใช้ `camelCase` (เช่น `memtableThreshold`, `blockCache`)
    *   ตัวแปรที่ไม่ได้ export (private) ให้ขึ้นต้นด้วยตัวพิมพ์เล็ก
    *   ตัวแปรที่ export (public) ให้ขึ้นต้นด้วยตัวพิมพ์ใหญ่
    *   **ความยาวชื่อตัวแปร:** ใช้ชื่อตัวแปรที่สั้นสำหรับ Scope ที่จำกัด (เช่น `i` สำหรับ loop index, `r` สำหรับ reader) และใช้ชื่อที่สื่อความหมายมากขึ้นสำหรับ Scope ที่กว้างขึ้น
*   **Getters:**
    *   ฟังก์ชันที่ทำหน้าที่เป็น Getter ไม่จำเป็นต้องมี Prefix `Get` เช่น เมธอดสำหรับ field `owner` ควรชื่อ `Owner()` ไม่ใช่ `GetOwner()`
*   **Interfaces:**
    *   สำหรับ Interface ที่มีเมธอดเดียว มักจะตั้งชื่อตามชื่อเมธอดแล้วต่อท้ายด้วย `-er` (เช่น `Reader`, `Writer`)
    *   สำหรับ Interface ที่มีหลายเมธอด ให้ตั้งชื่อตามหน้าที่ของมัน (เช่น `iterator.Interface`)
    *   หลีกเลี่ยงการใช้ `I` เป็น prefix (เช่น `IStorageEngine`)
*   **Constants:**
    *   ใช้ `MixedCaps` หรือ `camelCase` (เช่น `MaxL0Files`, `defaultBlockSize`)

## 4. การจัดการข้อผิดพลาด (Error Handling)

*   **Return Value:** Error ควรเป็นค่าที่คืนกลับมาเป็นลำดับสุดท้ายของฟังก์ชันเสมอ
*   **Error Wrapping:**
    *   ใช้ `fmt.Errorf("...: %w", err)` เพื่อเพิ่ม context ให้กับ error และรักษา error เดิมไว้
    *   **ข้อความ Error:** ไม่ต้องขึ้นต้นด้วยตัวพิมพ์ใหญ่ และไม่ต้องลงท้ายด้วยเครื่องหมายวรรคตอน
    *   **ตัวอย่าง:** `return fmt.Errorf("failed to open sstable %d: %w", id, err)`
*   **Error Checking:**
    *   ใช้ `errors.Is()` เพื่อตรวจสอบ error ที่เฉพาะเจาะจง (เช่น `errors.Is(err, sstable.ErrNotFound)`)
    *   ใช้ `errors.As()` เมื่อต้องการเข้าถึงข้อมูลภายใน error ที่ถูก wrap ไว้
*   **Handle Errors Immediately:** จัดการกับ error ทันทีที่ฟังก์ชันคืนค่ากลับมา อย่าละเลย error โดยใช้ `_` เว้นแต่จะมีความตั้งใจที่ชัดเจน

## 5. การบันทึก Log (Logging)

*   **Standard Library:** ใช้ `log/slog` ซึ่งเป็นไลบรารีมาตรฐานของ Go
*   **Structured Logging:**
    *   ใช้การบันทึก Log แบบมีโครงสร้าง (Key-Value pairs) เพื่อให้ง่ายต่อการค้นหาและวิเคราะห์
    *   **ตัวอย่าง:** `logger.Info("compaction completed", "level", 1, "duration_sec", 15.2)`
*   **Component Tag:** ทุกๆ Log ควรมี `component` เพื่อระบุว่า Log นั้นมาจากส่วนไหนของระบบ
    *   **ตัวอย่าง:** `logger.With("component", "StorageEngine")`
*   **Log Levels:**
    *   `Debug`: สำหรับข้อมูลการดีบักอย่างละเอียด
    *   `Info`: สำหรับเหตุการณ์สำคัญในการทำงานปกติ (เช่น การเริ่ม/หยุด service, การทำ compaction)
    *   `Warn`: สำหรับเหตุการณ์ที่ไม่คาดคิดแต่ยังไม่ถึงขั้นเป็น error (เช่น การ fallback ไปใช้ค่า default)
    *   `Error`: สำหรับข้อผิดพลาดที่เกิดขึ้นและควรได้รับการตรวจสอบ

## 6. การเขียนเทส (Testing)

*   **Package:** ใช้ `testing` package ที่เป็นมาตรฐาน
*   **File Naming:** ไฟล์เทสต้องลงท้ายด้วย `_test.go`
*   **Sub-tests:** ใช้ `t.Run()` เพื่อจัดกลุ่มเทสที่เกี่ยวข้องกัน ทำให้ผลลัพธ์อ่านง่ายและสามารถรันเทสย่อยได้
*   **Test Helpers:** ฟังก์ชันช่วยทดสอบควรเรียก `t.Helper()` ที่บรรทัดแรก
*   **Temporary Data:** ใช้ `t.TempDir()` เพื่อสร้างไดเรกทอรีชั่วคราวสำหรับไฟล์เทส ซึ่งจะถูกลบโดยอัตโนมัติเมื่อเทสเสร็จสิ้น
*   **Assertions:** ไม่มีการใช้ assertion library ภายนอก ให้ใช้ `if-t.Errorf/t.Fatalf` ตามปกติ
*   **Concurrency:** ใช้ `t.Parallel()` สำหรับเทสที่สามารถทำงานพร้อมกันได้เพื่อลดเวลาในการทดสอบ

## 7. การทำงานพร้อมกัน (Concurrency)

*   **Synchronization:**
    *   ใช้ `sync.Mutex` หรือ `sync.RWMutex` เพื่อป้องกันการเข้าถึงข้อมูลที่ใช้ร่วมกัน (Shared State)
    *   ควรมี comment ระบุว่า lock ตัวไหนป้องกันตัวแปรใดบ้าง
*   **Channels:** ใช้ Channel สำหรับการสื่อสารระหว่าง Goroutines (เช่น `flushChan`, `shutdownChan`)
*   **WaitGroup:** ใช้ `sync.WaitGroup` เพื่อรอให้ Goroutines ที่ทำงานเบื้องหลังทำงานจนเสร็จสิ้นก่อนปิดระบบ

## 8. การออกแบบ API (API Design)

*   **gRPC:**
    *   ปฏิบัติตาม Google API Design Guide และแนวปฏิบัติของ Protobuf
    *   ชื่อ Service, RPC, และ Message ควรชัดเจนและสื่อความหมาย
    *   ใช้ `stream` สำหรับ RPC ที่อาจมีการส่งข้อมูลจำนวนมาก (เช่น `Query`)
*   **Context:** ทุกฟังก์ชันที่อาจมีการทำงานที่ใช้เวลานานหรือมีการ I/O ควรรับ `context.Context` เป็นพารามิเตอร์ตัวแรก

## 9. เอกสาร (Documentation)

*   **Godoc:**
    *   ฟังก์ชัน, Type, และตัวแปรที่ export ทั้งหมดต้องมี Godoc comment ที่อธิบายการทำงาน, พารามิเตอร์, และค่าที่คืนกลับ
    *   Comment ควรเริ่มต้นด้วยชื่อของสิ่งที่มันอธิบาย และเป็นประโยคที่สมบูรณ์ (เช่น `// Close closes the connection.`)
*   **Code Comments:**
    *   ใช้ `//` สำหรับ comment ภายในฟังก์ชัน
    *   อธิบาย "ทำไม" (Why) ไม่ใช่แค่ "ทำอะไร" (What) สำหรับโค้ดที่ซับซ้อน

---

*เอกสารนี้สามารถปรับปรุงได้ตามความเหมาะสมเมื่อโปรเจกต์มีการพัฒนาต่อไป*