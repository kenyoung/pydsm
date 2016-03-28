#!/usr/bin/env python
import pydsm
sum = 0
count = 0
for j in xrange(1000):
  for i in xrange(7500):
    d = pydsm.read('hcn','DSM_AS_SCANS_REMAINING_L')
    count += 1
    sum += d[0]
    CSOWeather =  pydsm.read('colossus','CSO_METEOROLOGY_X')
    n = ((i+j) % 12) + 1
    crateName = 'crate%d' % (n)
    crate1 = pydsm.read(crateName, 'crate_to_hal_x')
  print 'Loop ',j,' done ', count, sum, d, CSOWeather['TEMP_F'],crate1['SCAN_NO_L'],crateName


