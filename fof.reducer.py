#!/usr/bin/env python
#Python Reducer for FOF Map/Reduce
import sys

key = ["", ""]
values = set()
first = True
write = False

for line in sys.stdin.readlines():
  tokens = line.split()

  match = tokens[0] == key[0] and tokens[1] == key[1]

  if not match:
    if write and not first:
      write = False
      for value in values:
        sys.stdout.write(value + " " + key[0] + " " + key[1] + "\n")

    else:
      first = False

    key = [tokens[0], tokens[1]]
    values = set(tokens[2:])

  else:
    values = values.intersection(tokens[2:])
    write = True
