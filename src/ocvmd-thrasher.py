#!/usr/bin/env python
import os, os.path, sys, re, commands, pickle, tempfile, getopt
import socket, string, random, threading, time, traceback

def usage():
    print 'usage:', os.path.basename(sys.argv[0]) + ' [-r <INT>] <file.dim> [canvas_width canvas_height xlow ylow xhigh yhigh zslice reduction_factor]'
    sys.exit(1)

try:
    opts, args = getopt.getopt(sys.argv[1:], "hr:v", ["help", "runs="])
except getopt.GetoptError:
    usage()
runs = None
verbose = False
for opt, arg in opts:
    if opt == "-v":
        verbose = True
    if opt in ("-h", "--help"):
        usage()
    if opt in ("-r", "--runs"):
        runs = int(arg)

if (len(args) != 1 and len(args) != 9):
    usage()

force_viewport = 0
if len(args) == 9:
    force_viewport = 1

def get_image_info(file_contents):
    lines = file_contents.split('\n')
    hash = {}
    for l in lines:
        l = l.strip()
        if len(l)>0:
            k,v = l.split(' ',1)
            hash[k] = v
    return tuple([hash['type']] + \
                 map(lambda x: long(hash[x]),
                     ['pixels_x','pixels_y','pixels_z',
                      'chunks_x','chunks_y','chunks_z']))
if not runs:
    runs = 100
    
z_slice = 0

random.seed(1)

dimfile = args[0]
f = open(dimfile, "r")
file_contents = f.read()
f.close()

(image_type, pixels_x, pixels_y, pixels_z,
 chunks_x, chunks_y, chunks_z) = get_image_info(file_contents)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 48876))
f = s.makefile()

try:
    import psyco
    psyco.full()
except ImportError:
    pass

try:
    while runs:
        runs = runs - 1
        if not force_viewport:
            canvas_width = random.randint(1, 1600)
            canvas_height = random.randint(1, 1600)
        else:
            canvas_width = int(args[1])
            canvas_height = int(args[2])
        print 'canvas:', canvas_width, canvas_height
        f.write('fetch\n')
        f.write('bytes ' + `len(file_contents)` + '\n')
        f.write(file_contents)
        f.write('viewport_dimensions %d %d\n' %
                (canvas_width,
                 canvas_height))
        if force_viewport:
            (upper_left_x, upper_left_y, lower_right_x, lower_right_y,
             reduction_factor) = map(lambda x: int(x), args[3:7] + args[8:])
        elif pixels_x < canvas_width and pixels_y < canvas_height:
            (upper_left_x, upper_left_y, lower_right_x, lower_right_y,
             reduction_factor) = \
             0, 0, pixels_x-1, pixels_y-1, 1
        else:
            reduction_factor = 1
            w = canvas_width
            h = canvas_height
            while w < pixels_x or h < pixels_y:
                w += canvas_width
                h += canvas_height
                reduction_factor = reduction_factor + 1
            fit_all_on_screen_reduction_factor = reduction_factor
            print 'fit_all_on_screen:', fit_all_on_screen_reduction_factor, w, h
            while 1:
                reduction_factor = random.randint(
                    1, fit_all_on_screen_reduction_factor)
#                 reduction_factor = fit_all_on_screen_reduction_factor
                if reduction_factor == fit_all_on_screen_reduction_factor:
                    bbox_width_x = pixels_x
                    bbox_width_y = pixels_y
                    break
                else:
                    bbox_width_x = canvas_width * reduction_factor
                    bbox_width_y = canvas_height * reduction_factor
                    if bbox_width_x <= pixels_x and bbox_width_y <= pixels_y:
                        break
            print reduction_factor, pixels_x, pixels_y, bbox_width_x, bbox_width_y
            upper_left_x = random.randint(0, pixels_x - bbox_width_x)
            upper_left_y = random.randint(0, pixels_y - bbox_width_y)
            lower_right_x = upper_left_x + bbox_width_x - 1
            lower_right_y = upper_left_y + bbox_width_y - 1
        s = 'viewport_bbox %d %d %d %d %d %d\n' % \
            (upper_left_x, upper_left_y,
             lower_right_x, lower_right_y,
             z_slice, reduction_factor)
        print 'using: %d %d %d %d %d %d %d %d\n' % \
            (canvas_width, canvas_height, upper_left_x, upper_left_y,
             lower_right_x, lower_right_y, z_slice, reduction_factor)
        f.write(s)
        f.write('threshold r -1 g -1 b -1\n')
        f.flush()
        validation = f.readline().strip()
        if validation == 'validated 0':
            error_size = self.server_socket_file.readline().strip()
            error_size = int(error_size.split(' ')[1])
            errors = self.server_socket_file.read(error_size)
            print errors
            sys.exit(1)
        new_image_dimensions = f.readline().strip()
        (pixels_new_image_x,pixels_new_image_y) = map(lambda x: int(x), new_image_dimensions.split()[1:])
        print 'new_image_dimensions: ', new_image_dimensions
        data = f.read(3*pixels_new_image_x*pixels_new_image_y)
except KeyboardInterrupt:
    pass
f.write('close\n')
f.close()
