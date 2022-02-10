#!/usr/bin/env python3
import sys


def tc_to_atoms():
    atom_name = "link"
    with open("facts/base.facts") as f:
        for line in f:
            l = line.split()
            print(f'{atom_name}("a{l[0]}", "a{l[1]}").')


def pointsto_to_atoms():
    atom_names = ["assignAlloc", "primitiveAssign", "load", "store"]

    with open(f'facts/assignAlloc.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'assignAlloc("v{l[0]}", "h{l[1]}").')

    with open(f'facts/primitiveAssign.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'primitiveAssign("v{l[0]}", "v{l[1]}").')

    with open(f'facts/load.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'load("v{l[0]}", "v{l[1]}", "f{l[2]}").')

    with open(f'facts/store.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'store("v{l[0]}", "v{l[1]}", "f{l[2]}").')


def rsg_to_atoms():
    with open('facts/up.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'up("a{l[0]}", "a{l[1]}").')

    with open('facts/down.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'down("a{l[0]}", "a{l[1]}").')

    with open('facts/flat.facts', 'r') as in_file:
        for line in in_file:
            l = line.split()
            print(f'flat("a{l[0]}", "a{l[1]}").')


if sys.argv[1] == 'tc':
    tc_to_atoms()
elif sys.argv[1] == 'pointsto':
    pointsto_to_atoms()
elif sys.argv[1] == 'rsg':
    rsg_to_atoms()
else:
    print("unrecognised program")
