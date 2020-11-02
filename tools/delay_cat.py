import sys
import time

if __name__ == "__main__":
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    with open(input_filename, "r") as input_file:
        with open(output_filename, "w") as output_file:
            for line in input_file.readlines():
                output_file.write(line)
                output_file.flush()
                time.sleep(0.01)