package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Kush/Database-internals/DataStructures/B-trees/implementation"
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/DataStructures/B-trees/serializer"
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/pkg/serialization"
)

func main() {
	pager, err := pagination.NewPager("test.db")
	if err.IsNotEmpty() {
		fmt.Printf("Error initializing pager: %v\n", err)
		return
	}
	defer pager.Close()

	baseSerializer := serialization.NewBinarySerializer()
	serializer := serializer.NewTreeNodeSerializer[*node.TreeNode](baseSerializer)
	btree := implementation.NewBPlusTree(0, pager, serializer, baseSerializer)
	err = btree.Init()
	if err.IsNotEmpty() {
		fmt.Printf("Error initializing B+ Tree: %v\n", err)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("B+ Tree CLI: Enter commands like:")
	fmt.Println("  insert <key> <int-value>")
	fmt.Println("  search <key>")
	fmt.Println("  exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		tokens := strings.Fields(line)

		if len(tokens) == 0 {
			continue
		}

		switch tokens[0] {
		case "exit":
			return

		case "insert":
			if len(tokens) != 3 {
				fmt.Println("Usage: insert <key> <int-value>")
				continue
			}
			key := tokens[1]
			var val int64
			_, err := fmt.Sscanf(tokens[2], "%d", &val)
			if err != nil {
				fmt.Println("Invalid value; must be an integer")
				continue
			}
			errObj := btree.Insert(key, common.NewIntValue(val))
			if errObj.IsNotEmpty() {
				fmt.Printf("Insert error: %v\n", errObj)
			} else {
				fmt.Println("Inserted successfully.")
			}

		case "search":
			if len(tokens) != 2 {
				fmt.Println("Usage: search <key>")
				continue
			}
			key := tokens[1]
			val, errObj := btree.Search(key)
			if errObj.IsNotEmpty() {
				fmt.Printf("Search error: %v\n", errObj)
			} else if val.IsEmpty() {
				fmt.Println("Key not found.")
			} else {
				fmt.Printf("Found: %v\n", val)
			}

		default:
			fmt.Println("Unknown command. Use insert/search/exit.")
		}
	}
}
