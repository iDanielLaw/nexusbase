package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/INLOpen/nexusbase/auth"
	"golang.org/x/term"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	addCmd := flag.NewFlagSet("add", flag.ExitOnError)
	addUserFile := addCmd.String("file", "users.db", "Path to the user database file.")
	addUsername := addCmd.String("username", "", "Username to add.")
	addHashType := addCmd.String("hash-type", "bcrypt", "Password hash type to use when creating a new user file (bcrypt, sha256, sha512).")
	addRole := addCmd.String("role", "reader", "Role for the new user ('reader' or 'writer').")

	listCmd := flag.NewFlagSet("list", flag.ExitOnError)
	listUserFile := listCmd.String("file", "users.db", "Path to the user database file.")

	delCmd := flag.NewFlagSet("delete", flag.ExitOnError)
	delUserFile := delCmd.String("file", "users.db", "Path to the user database file.")
	delUsername := delCmd.String("username", "", "Username to delete.")

	switch os.Args[1] {
	case "add":
		addCmd.Parse(os.Args[2:])
		handleAdd(addCmd, *addUserFile, *addUsername, *addRole, *addHashType)
	case "list":
		listCmd.Parse(os.Args[2:])
		handleList(*listUserFile)
	case "delete":
		delCmd.Parse(os.Args[2:])
		handleDelete(delCmd, *delUserFile, *delUsername)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: user-admin <command> [arguments]")
	fmt.Println("Commands:")
	fmt.Println("  add   - Add a new user")
	fmt.Println("  list  - List all users")
	fmt.Println("  delete - Delete a user")
	fmt.Println("\nUse 'user-admin <command> -h' for more information on a specific command.")
}

func handleAdd(fs *flag.FlagSet, file, username, role, hashTypeStr string) {
	if username == "" {
		fmt.Println("Error: -username is required.")
		fs.Usage()
		os.Exit(1)
	}
	if role != auth.RoleReader && role != auth.RoleWriter {
		fmt.Printf("Error: -role must be either '%s' or '%s'.\n", auth.RoleReader, auth.RoleWriter)
		fs.Usage()
		os.Exit(1)
	}

	fmt.Print("Enter password: ")
	bytePassword, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Printf("\nError reading password: %v\n", err)
		os.Exit(1)
	}
	password := string(bytePassword)
	fmt.Println() // Newline after password input

	fmt.Print("Confirm password: ")
	bytePasswordConfirm, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Printf("\nError reading password confirmation: %v\n", err)
		os.Exit(1)
	}
	passwordConfirm := string(bytePasswordConfirm)
	fmt.Println()

	if password != passwordConfirm {
		fmt.Println("Error: Passwords do not match.")
		os.Exit(1)
	}

	users, existingHashType, err := auth.ReadUserFile(file)
	if err != nil {
		fmt.Printf("Error reading user file: %v\n", err)
		os.Exit(1)
	}

	var finalHashType auth.HashType
	isNewFile := (len(users) == 0)

	if isNewFile {
		// New file, use the hash type from the flag
		switch hashTypeStr {
		case "bcrypt":
			finalHashType = auth.HashTypeBcrypt
		case "sha256":
			finalHashType = auth.HashTypeSHA256
		case "sha512":
			finalHashType = auth.HashTypeSHA512
		default:
			fmt.Printf("Error: invalid -hash-type '%s'. Supported values are: bcrypt, sha256, sha512.\n", hashTypeStr)
			os.Exit(1)
		}
		fmt.Printf("Creating new user file with hash type: %s\n", hashTypeStr)
	} else {
		// Existing file, must use the same hash type
		finalHashType = existingHashType
		fmt.Printf("Adding user to existing file (hash type: %d)\n", finalHashType)
	}

	if _, exists := users[username]; exists {
		fmt.Printf("Error: User '%s' already exists.\n", username)
		os.Exit(1)
	}

	hashedPassword, err := auth.HashPassword(password, finalHashType)
	if err != nil {
		fmt.Printf("Error hashing password: %v\n", err)
		os.Exit(1)
	}

	users[username] = auth.UserRecord{
		Username:     username,
		PasswordHash: hashedPassword,
		Role:         role,
	}

	if err := auth.WriteUserFile(file, users, finalHashType); err != nil {
		fmt.Printf("Error writing user file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully added user '%s' with role '%s' to %s.\n", username, role, file)
}

func handleList(file string) {
	users, hashType, err := auth.ReadUserFile(file)
	if err != nil {
		fmt.Printf("Error reading user file: %v\n", err)
		os.Exit(1)
	}

	if len(users) == 0 {
		fmt.Println("No users found.")
		return
	}

	fmt.Printf("Users (HashType: %d):\n", hashType)
	fmt.Println("------")
	for _, user := range users {
		fmt.Printf("- Username: %s, Role: %s\n", user.Username, user.Role)
	}
}

func handleDelete(fs *flag.FlagSet, file, username string) {
	if username == "" {
		fmt.Println("Error: -username is required.")
		fs.Usage()
		os.Exit(1)
	}

	users, hashType, err := auth.ReadUserFile(file)
	if err != nil {
		fmt.Printf("Error reading user file: %v\n", err)
		os.Exit(1)
	}

	if _, exists := users[username]; !exists {
		fmt.Printf("Error: User '%s' not found.\n", username)
		os.Exit(1)
	}

	delete(users, username)

	if err := auth.WriteUserFile(file, users, hashType); err != nil {
		fmt.Printf("Error writing user file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully deleted user '%s' from %s.\n", username, file)
}
