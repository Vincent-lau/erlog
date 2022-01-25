import sys

max_size = int(sys.argv[1])

with open('facts/hep-th-citations', 'r') as in_file:
    with open('facts/citations.facts', 'w') as out_file:
        i = 1
        for line in in_file:
            if i > max_size:
                break
            l = line.split()

            out_file.write(f"link(\"a{int(l[0])}\", \"a{int(l[1])}\").\n")
            i += 1
