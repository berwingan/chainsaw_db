package main

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
)

func fake_main() {
	// Data to send to Python (two numbers)
	a := 3.5
	b := 2.5

	// Convert numbers to strings and pass them to Python
	input := fmt.Sprintf("%f\n%f", a, b)

	// Create the Python process
	cmd := exec.Command("python3", "calc.py")

	// Set up the stdin to send the numbers to Python
	cmd.Stdin = strings.NewReader(input)

	// Setup for capturing stdout from the Python script
	output, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	// Print out the result from Python
	fmt.Println(string(output))
}
