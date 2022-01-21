#!/usr/bin/env python3

atom_name = "link"
with open ("facts/base.facts") as f:
  for line in f:
    l = line.split()
    print(f'{atom_name}("a{l[0]}", "a{l[1]}").')

