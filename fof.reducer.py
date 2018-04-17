#!/usr/bin/env python
#Python Reducer for FOF Map/Reduce
import sys

triple = ["", "", ""]
count = 0
first = True

for line in sys.stdin.readlines():
  kv = line.split("\t")
  key = kv[0].split()
  value = int(kv[1])

  match = triple[0] == key[0] and triple[1] == key[1] and triple[2] == key[2]

  if match:
    count += value
  else:
    if count == 3:
      sys.stdout.write(triple[0] + " " + triple[1] + " " + triple[2] + "\n")
      sys.stdout.write(triple[1] + " " + triple[0] + " " + triple[2] + "\n")
      sys.stdout.write(triple[2] + " " + triple[0] + " " + triple[1] + "\n")
    count = value
    triple = key
