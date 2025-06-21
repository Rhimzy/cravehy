# sort_numbers.py

# Read numbers from the file
with open('pids_from_log.txt', 'r') as f:
    numbers = [int(line.strip()) for line in f if line.strip()]

# Sort the numbers
numbers.sort()

# Write the sorted numbers to a new file
with open('sorted_output.txt', 'w') as f:
    for number in numbers:
        f.write(f"{number}\n")

print("Numbers sorted and written to sorted_output.txt")
