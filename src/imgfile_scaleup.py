#!/usr/bin/env python
import os, os.path, sys, re, commands, pickle, tempfile, getopt
import socket, string, random, threading, time, traceback

def usage():
    print 'usage:', os.path.basename(sys.argv[0]) + ' <filename.img> <#increments>'
    sys.exit(1)

if ((len(sys.argv)-1) != 2):
    usage()

input_filename = sys.argv[1]
increments = int(sys.argv[2])
dirname = os.path.dirname(sys.argv[0])
factor = None

if not re.match(r'^.*\.[iI][mM][gG]$', input_filename):
    sys.stderr.write("ERROR: input an .IMG file!\n")
    sys.exit(1)

for incr in range(increments):
    file, ext = string.rsplit(input_filename, '.', 2)
    m = re.match("^(.*)x([0-9]+)$", file)
    if m:
        file = m.group(1)
        num = m.group(2)
        factor = int(num) * 4
    else:
        factor = 4
    output_filename = '%sx%d.IMG' % (file, factor)
    cmd = '%s %s %s' % (os.path.join(dirname, 'imgfile_scaler'),
                        input_filename, output_filename)
    print '+%s' % (cmd)
    rc = os.system(cmd)
    if rc:
        sys.stderr.write("ERROR: %s returned %d" % (cmd, rc))
        sys.exit(1)
    input_filename = output_filename
