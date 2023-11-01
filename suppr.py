
import csv
import os
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Utilisation : python3 suppr.py <nom_du_fichier.csv>")
        sys.exit(1)
    with open(os.path.splitext(sys.argv[1])[0]+"_clean.csv", 'w+') as output_file:
        with open(sys.argv[1]) as input_file:  # change you file name here
            reader = csv.reader(input_file, delimiter='\n')

            line_index = 0  # debugging

            for row in reader:
                line_index += 1
                line = row[0]
                if 'DEL' not in line:
                    output_file.write(line)
                    output_file.write('\n')

            # lines_to_write = [row[0] for row in reader if 'DEL' not in row[0]]
            # output_file.write('\n'.join(lines_to_write) + '\n')
