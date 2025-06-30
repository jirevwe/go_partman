package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var example string
	flag.StringVar(&example, "example", "simplified", "Example to run: root, simple, separate, auth, simplified")
	flag.Parse()

	// Run the selected example
	switch example {
	case "root":
		fmt.Println("Running Example: Root Mount")
		ExampleRootMount()
	case "simple":
		fmt.Println("Running Example: Simple Integration")
		ExampleSimpleIntegration()
	case "separate":
		fmt.Println("Running Example: Separate Handlers")
		ExampleSeparateHandlers()
	case "auth":
		fmt.Println("Running Example: With Authentication")
		ExampleWithAuth()
	case "simplified":
		fmt.Println("Running Example: Simplified API")
		ExampleSimplifiedAPI()
	default:
		fmt.Printf("Unknown example: %s\n", example)
		fmt.Println("Available examples:")
		fmt.Println("  -root      : Mount UI at root path")
		fmt.Println("  -simple    : Simple integration with UI handler")
		fmt.Println("  -separate  : Separate handlers for API and UI")
		fmt.Println("  -auth      : With authentication middleware")
		fmt.Println("  -simplified: Simplified API (default)")
		fmt.Println("\nUsage: go run . -example=<example_name>")
		os.Exit(1)
	}
}
