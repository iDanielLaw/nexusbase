package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/INLOpen/nexusbase/sys"
)

const outputFileName = "combined_go_project_code_with_lines.log"

// รูปแบบ (Pattern) ที่จำเป็นซึ่งอาจจะไม่ได้อยู่ใน .gitignore เสมอไป
var essentialExcludePatterns = []string{
	// ไฟล์ที่ถูกสร้างขึ้นอัตโนมัติ (Go Generated files)
	`_grpc\.pb\.go$`,
	`\.pb\.go$`,
	// ไฟล์ทดสอบ (Test files)
	`_test\.go$`,
	// ไฟล์ผลลัพธ์ของสคริปต์นี้เอง
	`^` + regexp.QuoteMeta(outputFileName) + `$`,
	// โฟลเดอร์ .git
	`^\.git/`,
}

func main() {

	projectRoot, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}

	var combinedContent strings.Builder

	// โหลด Pattern จาก .gitignore
	gitignorePatterns, err := loadGitignorePatterns(projectRoot)
	if err != nil {
		// ไม่ใช่ Error ร้ายแรงหากไม่มีไฟล์ .gitignore, แค่แสดงคำเตือน
		fmt.Printf("Warning: could not load .gitignore: %v\n", err)
	}

	// รวม Pattern ที่จำเป็นกับ Pattern จาก .gitignore
	excludePatterns := append(essentialExcludePatterns, gitignorePatterns...)

	// คอมไพล์ Regex ทั้งหมด
	compiledExcludes := make([]*regexp.Regexp, len(excludePatterns))
	for i, pattern := range excludePatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			fmt.Printf("Error compiling regex pattern '%s': %v\n", pattern, err)
			os.Exit(1)
		}
		compiledExcludes[i] = re
	}

	err = filepath.Walk(projectRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing path %s: %v\n", path, err)
			return err
		}

		relativePath, _ := filepath.Rel(projectRoot, path)
		relativePath = filepath.ToSlash(relativePath)

		// ตรวจสอบว่า Path ตรงกับ Pattern ที่ยกเว้นหรือไม่
		for _, re := range compiledExcludes {
			if re.MatchString(relativePath) {
				if info.IsDir() {
					fmt.Printf("Skipping excluded directory: %s\n", relativePath)
					return filepath.SkipDir
				}
				return nil
			}
		}

		// ตรวจสอบเฉพาะไฟล์ .go เท่านั้น
		if strings.HasSuffix(info.Name(), ".go") {
			fmt.Printf("Reading: %s\n", relativePath)

			file, err := sys.Open(path)
			if err != nil {
				fmt.Printf("Error opening file %s: %v\n", relativePath, err)
				return nil
			}
			defer file.Close()
			combinedContent.WriteString(fmt.Sprintf("\n\n/* --- Start of file: %s --- */\n", relativePath))

			scanner := bufio.NewScanner(file)
			lineNumber := 1
			for scanner.Scan() {
				// เพิ่มเลขบรรทัดนำหน้าแต่ละบรรทัดโค้ด
				combinedContent.WriteString(fmt.Sprintf("%d: %s\n", lineNumber, scanner.Text()))
				lineNumber++
			}

			if err := scanner.Err(); err != nil {
				fmt.Printf("Error scanning file %s: %v\n", relativePath, err)
			}

			combinedContent.WriteString(fmt.Sprintf("/* --- End of file: %s --- */\n", relativePath))

		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking through directory: %v\n", err)
		os.Exit(1)
	}

	// เขียนเนื้อหาทั้งหมดลงในไฟล์ output
	outputPath := filepath.Join(projectRoot, outputFileName)
	err = os.WriteFile(outputPath, []byte(combinedContent.String()), 0644)
	if err != nil {
		fmt.Printf("Error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nSuccessfully combined all Go code (with line numbers) into: %s\n", outputFileName)
	fmt.Printf("File is located at: %s\n", outputPath)
}

// loadGitignorePatterns อ่านไฟล์ .gitignore และแปลงแต่ละบรรทัดเป็น pattern ของ regular expression
func loadGitignorePatterns(root string) ([]string, error) {
	gitignorePath := filepath.Join(root, ".gitignore")
	file, err := sys.Open(gitignorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // ไม่ใช่ error หากไม่มีไฟล์ .gitignore
		}
		return nil, err
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		regex := gitignoreLineToRegex(line)
		patterns = append(patterns, regex)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	fmt.Printf("Loaded %d patterns from .gitignore\n", len(patterns))
	return patterns, nil
}

// gitignoreLineToRegex แปลง gitignore pattern หนึ่งบรรทัดให้เป็น regular expression
// นี่เป็นเวอร์ชันที่เรียบง่ายและจัดการกับกรณีส่วนใหญ่ได้
func gitignoreLineToRegex(line string) string {
	// ถ้า pattern ลงท้ายด้วย / หมายถึงเป็น directory
	// เราจะทำให้มัน match กับ directory นั้นๆ และทุกอย่างที่อยู่ข้างใน
	if strings.HasSuffix(line, "/") {
		// `build/` -> `^build/`
		return `^` + regexp.QuoteMeta(line)
	}

	// ถ้า pattern ขึ้นต้นด้วย / หมายถึงให้ match จาก root เท่านั้น
	if strings.HasPrefix(line, "/") {
		// `/config.yaml` -> `^config\.yaml$`
		return `^` + regexp.QuoteMeta(line[1:]) + `$`
	}

	// ถ้าเป็น pattern ทั่วไป (เช่น `data` หรือ `*.log`)
	// `*.log` -> `\.log$`
	// `data` -> `(^|/)data$`
	line = regexp.QuoteMeta(line)
	line = strings.ReplaceAll(line, `\*`, `.*`) // แปลง glob star เป็น regex
	return `(^|/)` + line + `$`
}
