#!/usr/bin/env python
#Python Mapper for FOF Map/Reduce
import sys

for line in sys.stdin.readlines():
  tokens = line.split()

  user = tokens[0]
  friends = sorted(tokens[1:len(tokens)])

  friends_str = ""
  for friend in friends:
    friends_str = friends_str + friend + " "

  friends_str = friends_str[:-1]

  for friend in friends:
    if user < friend:
      sys.stdout.write(user + " " + friend + " " + friends_str + "\n")
    else:
      sys.stdout.write(friend + " " + user + " " + friends_str + "\n")
