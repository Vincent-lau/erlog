from os import write


with open('hep-th-citations', 'r') as in_file:
  with open('tc-large', 'w') as out_file:
    i = 1
    for line in in_file:
      if i > 500:
        break
      l = line.split()

      out_file.write(f"link(\"a{int(l[0])}\", \"a{int(l[1])}\")\n") 
      i += 1
