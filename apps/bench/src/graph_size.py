def get_graph_size(file_name):
  num_nodes = 0
  num_edges = 0
  with open(file_name, 'r') as in_file:
    for line in in_file:
      if line[0:4] != 'link':
        continue
      num_edges += 1
      l = line.strip("link.\n()")
      n1, n2 = l.split(",")
      n1 = n1.strip('a"')
      n2 = n2.strip('a" ')
      num_nodes = max(num_nodes, max(int(n1), int(n2)))
      print(n1, n2)
  return (num_nodes, num_edges)

num_nodes, num_edges = get_graph_size("../examples/tc-large.dl")
print(f"#nodes {num_nodes} #edges {num_edges}")
