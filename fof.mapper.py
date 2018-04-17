#!/usr/bin/env python
#Python Mapper for FOF Map/Reduce
import sys

for line in sys.stdin.readlines():
  tokens = line.split()

  user = tokens[0]
  friends = tokens[1:len(tokens)]

  for i in range(0,len(friends)):
    for j in range(i+1,len(friends)):
      key = sorted([int(user), int(friends[i]), int(friends[j])])
      sys.stdout.write(str(key[0]) + " " + str(key[1]) + " " + str(key[2]) + "\t1\n")
