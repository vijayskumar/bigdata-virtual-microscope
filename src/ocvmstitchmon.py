#!/usr/bin/env python
import sys, os, os.path, socket, time, threading, re, Queue, string, commands
from Tkinter import *

sys.path.append(os.path.dirname(sys.argv[0]))
import Pmw

def usage():
    print 'usage:', os.path.basename(sys.argv[0]) + ' [tcp_listen_port]'
    sys.exit(1)

if ((len(sys.argv)-1) > 1):
    usage()
elif ((len(sys.argv)-1) == 1) and sys.argv[1]=='-h':
    usage()

# globals
port = 49992
fontsize = 9
root = None
frametop = None
screenwidth = 0
screenheight = 0
the_text =None
quitting = 0
qmutex = threading.Semaphore()
workq = Queue.Queue(0)

if (len(sys.argv)-1 == 1):
    port = int(sys.argv[1])

host_color = {}
unused_colors_orig = ['red','orange','green1','green4','blue3',
                      'black','white',
                      'yellow1','yellow4',
                      'wheat1',
                      'purple1',
                      'gold1',
                      'maroon1',
                      'maroon4',
                      'salmon',
                      'cornflower blue',
                      'grey0',
                      'grey5',
                      'grey10',
                      'grey15',
                      'grey20',
                      'grey25',
                      'grey30',
                      'grey35',
                      'grey40',
                      'grey45',
                      'grey50',
                      'grey55',
                      'grey60',
                      'grey65',
                      'grey70',
                      'grey75',
                      'grey80',
                      'grey85',
                      'grey90',
                      'grey95',
                      'grey100']
unused_colors = unused_colors_orig + [] # copy it
mutex = threading.Semaphore()

def do_setup(line):
    global host_color
    global unused_colors
    mutex.acquire()
    host_color = {}
    unused_colors = unused_colors_orig + []
    (xres, yres) = line.split()
    xres = int(xres)
    yres = int(yres)
    the_text.config(state=NORMAL)
    the_text.delete('1.0',END)
    for y in range(yres):
        for x in range(xres):
            the_text.insert(END, '  ')
        the_text.insert(END, '\n')
        for x in range(xres):
            the_text.insert(END, ' .')
        the_text.insert(END, '\n')
    the_text.config(state=DISABLED)
    mutex.release()

def do_mod(line):
    (hostname, x1, y1, left_to_right) = line.split()
    x1 = int(x1)
    y1 = int(y1)
    if left_to_right == '1':
        left_to_right = 1
    else:
        left_to_right= 0

    mutex.acquire()
    the_text.config(state=NORMAL) 
    if not host_color.has_key(hostname):
        color = unused_colors[0]
        unused_colors.pop(0)
        host_color[hostname] = color
        the_text.insert(END, '\n' + hostname)
        the_text.tag_add(hostname,
                         END + '-' + `len(hostname) + 1` + 'c',
                         END + '-1c')
        the_text.tag_config(hostname, foreground = host_color[hostname])
        
    if left_to_right:
        pos1 = '%d.%d' % ((y1+1)*2, (x1+1)*2)
        pos2 = '%d.%d' % ((y1+1)*2, (x1+1)*2 + 1)
        the_text.delete(pos1, pos2)
        the_text.insert(pos1, '_')
        the_text.tag_add(hostname, pos1, pos2)
    else:
        pos1 = '%d.%d' % ((y1+1)*2+ 1, (x1+ 1)*2-1)
        pos2 = '%d.%d' % ((y1+1)*2+ 1, (x1+ 1)*2)
        the_text.delete(pos1, pos2)
        the_text.insert(pos1, '|')
        the_text.tag_add(hostname, pos1, pos2)
        the_text.tag_config(hostname, foreground = host_color[hostname])
    the_text.config(state=DISABLED)
    mutex.release()

class clienthandler(threading.Thread):
    def __init__(self, s):
        threading.Thread.__init__(self)
        self.s = s
    def run(self):
        f = self.s.makefile('r')
        while 1:
            line = f.readline()
            if not line:
                break
            line = line.strip()
#             print 'line is "%s"' % (line)
            terms = string.split(line, ' ', 1)
            if len(terms) == 2:
                (type, args) = terms[0], terms[1]
                workq.put([terms[0], terms[1]])
            else:
                print 'protocol error for line', line
                break
        f.close()

class accepter(threading.Thread):
    def __init__(self, listen_socket):
        threading.Thread.__init__(self)
        self.listen_socket = listen_socket
    def run(self):
        while 1:
            qmutex.acquire()
            if quitting:
                break
            qmutex.release()
            (csock, addr) = self.listen_socket.accept()
            t = clienthandler(csock)
            t.start()

def workq_handler():
    while workq.qsize():
        try:
            msg = workq.get_nowait()
            if not msg:
                return # we were told to close by giving us a NULL,
                       # so return without reregistering myself
            if msg[0] == 'setup':
                do_setup(msg[1])
            elif msg[0] == 'edgein':
                do_mod(msg[1])
            else:
                print "ERROR: internal protocol failure"
                sys.exit(1)
        except Queue.Empty:
            pass

    root.after(200, workq_handler) # re-start itself

def main():
    # start accepter thread
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('',port))
        s.listen(100)
    except Exception:
        print 'cannot bind to port', port
        sys.exit(1)
    t = accepter(s)
    t.start()

    # lay out gui
    global root
    root = Pmw.initialise()
    root.title('ocvmstitchmon on ' + commands.getoutput("hostname"))
    root.geometry('+50+50')

    # winfo_screenwidth and winfo_screenheight
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()

    global fontsize
    if screenwidth > 1280:
        fontsize = fontsize + 1

    fr = Pmw.ScrolledFrame(root, usehullsize=1,
                           hull_width=(screenwidth-64) / 2,
                           hull_height=screenheight/2)
    fr.pack(expand=1,fill="both",side=BOTTOM)
    global frametop
    frametop = fr.interior()

    global the_text
    the_text=Text(frametop,height=500,width=500,state=DISABLED,wrap='none',font=('courier',fontsize,'normal'))
    the_text.pack(expand=1,fill="both",side=TOP)

    root.after(200, workq_handler)
    root.mainloop()
    workq.put(None)

    # make accepter thread quit so program can exit
    qmutex.acquire()
    global quitting
    quitting = 1
    qmutex.release()
    closer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    closer.connect(("localhost", port))
    closer.close()

if __name__ == "__main__": # when run as a script
    main()
