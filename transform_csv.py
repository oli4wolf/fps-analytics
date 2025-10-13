import csv
import re

def transform_csv(input_file, output_file):
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write header for the new CSV
        writer.writerow(['timestamp', 'message'])

        # Skip the header row of the input file
        next(reader)

        for row in reader:
            if len(row) > 7:  # Ensure _raw field exists
                raw_data = row[7]
                
                # Cut everything before "timestamp="
                match = re.search(r'timestamp=(.*)', raw_data)
                if match:
                    relevant_data = match.group(1)
                    
                    # Split into timestamp and message
                    # The timestamp format is "YYYY-MM-DD HH:MM:SS,ms"
                    # The message starts after "message="
                    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) loglevel=INFO  class=.*? message=(.*)', relevant_data)
                    if timestamp_match:
                        timestamp = timestamp_match.group(1)
                        message = timestamp_match.group(2)
                        writer.writerow([timestamp, message])
                    else:
                        # Handle cases where the timestamp/message pattern is not found
                        print(f"Could not parse timestamp and message from: {relevant_data}")
                else:
                    print(f"Could not find 'timestamp=' in _raw field: {raw_data}")

if __name__ == "__main__":
    input_csv_file = 'short_data.csv'
    output_csv_file = 'transformed_data.csv'
    transform_csv(input_csv_file, output_csv_file)