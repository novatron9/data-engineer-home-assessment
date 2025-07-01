#!/usr/bin/env python3
"""
Task A: Social Media Post Timestamp Difference Calculator

Problem: Calculate absolute difference between two timestamps with timezone support.
Format: Day dd Mon yyyy hh:mm:ss +xxxx
Output: Absolute difference in seconds
"""

from datetime import datetime
import sys


def parse_timestamp(timestamp_str):
    """
    Parse timestamp string in format: Day dd Mon yyyy hh:mm:ss +xxxx
    Returns datetime object with timezone info
    """
    # Remove day name and parse the rest
    parts = timestamp_str.strip().split()
    
    # Format: Day dd Mon yyyy hh:mm:ss +xxxx
    day = int(parts[1])
    month_str = parts[2]
    year = int(parts[3])
    time_str = parts[4]
    timezone_str = parts[5]
    
    # Month mapping
    months = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    month = months[month_str]
    
    # Parse time
    hour, minute, second = map(int, time_str.split(':'))
    
    # Parse timezone offset
    tz_sign = 1 if timezone_str[0] == '+' else -1
    tz_hours = int(timezone_str[1:3])
    tz_minutes = int(timezone_str[3:5])
    
    # Create datetime object (naive)
    dt = datetime(year, month, day, hour, minute, second)
    
    # Convert to UTC by subtracting the timezone offset
    total_offset_minutes = tz_sign * (tz_hours * 60 + tz_minutes)
    total_offset_seconds = total_offset_minutes * 60
    
    # Convert to UTC timestamp
    utc_timestamp = dt.timestamp() - total_offset_seconds
    
    return utc_timestamp


def calculate_time_difference(t1_str, t2_str):
    """
    Calculate absolute difference between two timestamp strings
    Returns difference in seconds
    """
    t1_utc = parse_timestamp(t1_str)
    t2_utc = parse_timestamp(t2_str)
    
    return abs(int(t1_utc - t2_utc))


def process_test_cases(input_text):
    """
    Process multiple test cases from input text
    Returns list of results
    """
    lines = input_text.strip().split('\n')
    t = int(lines[0])
    results = []
    
    for i in range(t):
        t1 = lines[1 + i * 2]
        t2 = lines[2 + i * 2]
        diff = calculate_time_difference(t1, t2)
        results.append(str(diff))
    
    return results


def main():
    """Main function to handle input and output"""
    if len(sys.argv) > 1:
        # Read from file if provided
        with open(sys.argv[1], 'r') as f:
            input_text = f.read()
    else:
        # Read from stdin
        print("Enter test cases (or press Ctrl+D when done):")
        input_text = sys.stdin.read()
    
    try:
        results = process_test_cases(input_text)
        for result in results:
            print(result)
    except Exception as e:
        print(f"Error processing input: {e}", file=sys.stderr)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())