#!/usr/bin/python2.7
import json
import sys




if len(sys.argv)<4:
  file = sys.argv[1]
  out_path = sys.argv[2]
else:
  print "usage: "+sys.argv[0]+" source_file destfile"
  exit(1)


latest = 0
with open(file) as f:
  while True:
    s = f.readline().replace('\n','')
    if s == '':
      break
    i = s.find(",")
    key = s[:i]
    strdoc = s[i+1:]
    doc = json.loads(strdoc)
    if 'date' in doc:
        if doc['date'] > latest:
		latest = doc['date']
  
print latest
f=open(out_path,'w')
f.write(str(latest))
f.close()
