#!/usr/bin/python
import json
import math

cpus = 0
with open('/proc/cpuinfo') as f:
    for line in f:
        if 'processor' in line:
            cpus += 1

meminfo = {}
with open('/proc/meminfo') as f:
    for line in f:
        meminfo[line.split(':')[0]] = line.split(':')[1].strip()

memory = int(meminfo['MemTotal'].split(' ')[0])/1000
memory = int(math.floor(memory / 1000.0))

info = {'cpus':cpus, 'memory':memory}
with open('/etc/prominence.json', 'w') as file:
    json.dump(info, file)
