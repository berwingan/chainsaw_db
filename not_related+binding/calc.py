# calc.py
import sys

def main():
    # Reading input arguments from stdin
    input_data = sys.stdin.read().splitlines()
    
    # Parse the numbers
    a = float(input_data[0])
    b = float(input_data[1])
    
    # Perform an operation (addition)
    result = a + b
    
    # Output the result to stdout
    print(f"Result: {result}")

if __name__ == "__main__":
    main()
