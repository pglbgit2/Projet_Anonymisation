import CSVManager
import os
import sys
import string
import random
from datetime import datetime

def pseudonymize(fileToReadName, fileToWriteName):
    tab = CSVManager.readCorrectlyCSVFile(fileToReadName)
    idWeekAno = {}
    
    for value in tab:
        # Extract id, timestamp, and week from the row
        id_value = value[0]
        timestamp_str = value[1]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        week_number = timestamp.isocalendar()[1]

        # Create a unique key for each id-week combination
        key = (id_value, week_number)

        if key not in idWeekAno:
            pseudo = generateString()
            idWeekAno[key] = pseudo
            value[0] = pseudo
        else:
            value[0] = idWeekAno[key]

    CSVManager.writeCorrectlyCSVFile(tab, fileToWriteName)

def generateString(size=7):
    authorized_chars = string.ascii_letters + string.digits
    generated_string = ''.join(random.choice(authorized_chars) for _ in range(size))
    return generated_string

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Usage: python3 {} <source.csv> <dest.csv>\n".format(sys.argv[0]))
        sys.exit(1)
    pseudonymize(sys.argv[1], sys.argv[2])
