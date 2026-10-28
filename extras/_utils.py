import os
from datetime import datetime

# Get timestamp Indonesian format
def get_timestamp():
    now = datetime.now()
    timestamp = now.strftime("%d-%m-%Y %H:%M:%S")
    return timestamp

def debug(message):
    timestamp = get_timestamp()
    print(f"{timestamp} - {message}")