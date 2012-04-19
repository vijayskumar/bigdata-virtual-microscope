#!/usr/bin/env python
import os, os.path, sys, re, commands, pickle, tempfile, sha
import socket, string, random, threading, time, traceback, inspect
import sys, os, os.path, socket, time, threading, re, Queue, string, commands

# Tkinter
from Tkinter import *
import tkFileDialog
import tkMessageBox

# Pmw
sys.path.append(os.path.dirname(sys.argv[0]))
import Pmw

# PIL
import Image
import ImageTk

def usage():
    print 'usage:', os.path.basename(sys.argv[0]) + ''
    sys.exit(1)

def error(message):
    print message
    tkMessageBox.showerror(message, message)

def notice(message):
    tkMessageBox.showinfo('ocvmgui notice', message)

def widget_enable(wid):
    wid.config(state=NORMAL)   

def mkdtempold():
    import random, os, os.path
    dir = '/tmp/mkdtempold.'
    n = 0
    while n < 100000:
        val = `random.randint(0, 1000000)`
        file = dir + val
        try:
            os.mkdir(file, 0700)
            return file
        except OSError, e:
            import errno
            if e.errno == errno.EEXIST:
                continue # try again
            raise
        n += 1
    raise RuntimeError, 'ERROR: could not find temporary filename'
    
root = None
file_opener_obj = None
command_line_files = None

pending_tk_events = []
pending_tk_events_mutex = threading.Semaphore()

def pending_tk_events_monitor():
    global pending_tk_events
    pending_tk_events_mutex.acquire()
    for funcall in pending_tk_events:
        funcall[0](*funcall[1:])
    pending_tk_events = []
    pending_tk_events_mutex.release()
    root.after(500, pending_tk_events_monitor)

def open_command_line_files():
    for f in command_line_files:
        file_opener_obj.open_dim_file(f)

active_image_frames = {}

class file_opener:
    def get_config_filename(self):
        if sys.platform == 'win32':
            return 'ocvmgui_prefs.txt'
        else:
            return os.path.join(os.getenv("HOME"), ".ocvmguirc")

    def get_cache_dir(self):
        use_existing = 0
        if self.config_values['cache_dir']:
            dir = self.config_values['cache_dir']
            if (os.path.exists(dir) and \
                (sys.platform == 'win32' or \
                 os.stat(dir)[4] == os.getuid())):
                use_existing = 1
        if not use_existing:
            self.config_values['cache_dir'] = mkdtempold()
        return self.config_values['cache_dir']
                
    def __init__(self):
        global file_opener_obj
        file_opener_obj = self

        self.image_canvas = None
        self.photo_objects = []
        self.image_object = None
        self.image_master = None
        self.photo_master = None
        self.photo_master_id = None

        self.config_values = {'cache_dir' : None,
                              'server_hostname' : None,
                              'server_port' : None}
        self.config_fn = self.get_config_filename()
        if os.path.exists(self.config_fn):
            f = open(self.config_fn, "r")
            while 1:
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                (key, val) = line.split(' ', 1)
                if self.config_values.has_key(key):
                    self.config_values[key] = val
                else:
                    sys.stderr.write("WARNING: unknown line " + line + \
                                     " in " + self.config_fn)

        self.root = Pmw.initialise()
        global root
        root = self.root
        root.after(500, pending_tk_events_monitor)
        self.root.title('ocvmgui on ' + socket.gethostname())
        w = Pmw.Group(self.root, tag_text='server settings')
        w.pack()
        self.hostname_entry = Pmw.EntryField(
            w.interior(),
            labelpos = 'w',
            label_text = 'hostname',
            value = (self.config_values['server_hostname'] or 'localhost'))
        self.port_entry = Pmw.EntryField(
            w.interior(),
            labelpos = 'w',
            label_text = 'port',
            value = (self.config_values['server_port'] or '48876'),
            validate = {'validator' : 'integer',
                        'min' : 1, 'max' : '65535'})
        entries = (self.hostname_entry, self.port_entry)
        for e in entries:
            e.pack(fill='x', expand=1, padx=10, pady=5, side=TOP)
        Pmw.alignlabels(entries)

        button_frame = Frame(self.root)
        button_frame.pack(side=TOP)
        Button(button_frame, text='stitch .img file',
               command=self.open_img_file_choose).pack(padx=10,pady=5,side=LEFT)
        Button(button_frame, text='view .dim file',
               command=self.open_dim_file_choose).pack(padx=10,pady=5,side=LEFT)

        self.root.resizable(0,0)
        self.root.protocol("WM_DELETE_WINDOW", self.exit)

        if len(command_line_files) > 0:
            self.root.after(200, open_command_line_files)
        
        self.root.mainloop()

    def open_img_file_choose(self):
        filename = tkFileDialog.askopenfilename(
            parent=self.root, title='Choose an .img file',
            filetypes=[('.img files','*.img'), ('.img files','*.IMG')])
        if filename == () or filename == '':
            return
        return self.open_img_file(filename)

    def open_img_file(self, filename):
        if not os.path.exists(filename):
            error('file '+ filename+' does not exist!')
            return
        if not re.match(r'^.*\.[iI][mM][gG]$', filename):
            error('I do not know how to handle ' + filename + \
                  '.  Please specify an .img file.')
            return
        sid = stitch_img_dialog(filename,
                                self.hostname_entry.getvalue(),
                                int(self.port_entry.getvalue()))

    def open_dim_file_choose(self):
        filename = tkFileDialog.askopenfilename(
            parent=self.root, title='Choose a .dim (Distributed IMage) file',
            filetypes=[('.dim files','*.dim'), ('.dim files')])
        if filename == () or filename == '':
            return
        return self.open_dim_file(filename)

    def open_dim_file(self, filename):
        if not os.path.exists(filename):
            error('file '+ filename+' does not exist!')
            return
        if not re.match(r'^.*\.dim$', filename):
            error('I do not know how to handle ' + filename + \
                  '.  Please specify a .dim (Distributed IMage) file.')
            return
        f = open(filename, "r")
        data = f.read()
        f.close()
        print "I got %d bytes from this file." % len(data)
        self.load_image_frame(filename)
        
    def load_image_frame(self, filename):
        ifnew = ImageFrame(self,
                           self.root, filename,
                           self.hostname_entry.getvalue(),
                           int(self.port_entry.getvalue()),
                           self.get_cache_dir())
        if not ifnew.startup_failed:
            active_image_frames[ifnew] = 1

    def exit(self):
        self.config_values['server_hostname'] = self.hostname_entry.getvalue()
        self.config_values['server_port'] = self.port_entry.getvalue()
        self.config_values['cache_dir'] = self.get_cache_dir()
        
        for fr in active_image_frames.keys():
            fr.exit()
        self.root.destroy()


        f = open(self.config_fn, "w")
        keys = self.config_values.keys()
        keys.sort()
        for k in keys:
            if self.config_values[k] != None:
                f.write(k + ' ' + self.config_values[k] + '\n')
        f.close()

class stitch_img_dialog_completion_waiter(threading.Thread):
    def __init__(self, stitch_img_dialog_class, original_filename,
                 output_filename, tempfn):
        threading.Thread.__init__(self)
        self.stitch_img_dialog_class = stitch_img_dialog_class
        self.original_filename = original_filename
        self.output_filename = output_filename
        self.tempfn = tempfn
    def run(self):
        line = self.stitch_img_dialog_class.server_socket_file.readline().\
               strip()
        (header, val) = line.split()
        self.stitch_img_dialog_class.server_socket_file.write('close\n')
        self.stitch_img_dialog_class.server_socket_file.flush()
        pending_tk_events_mutex.acquire()
        if val == "0":
            pending_tk_events.append(
                [notice,
                 'stitching of file %s to output file %s completed cleanly' %
                 (self.original_filename, self.output_filename)])
        else:
            pending_tk_events.append(
                [error,
                 ('stitching of file %s returned %s' % (self.original_filename,
                                                        val))])
        pending_tk_events_mutex.release()
        os.remove(self.tempfn)
                 
class stitch_img_dialog:
    def __init__(self, filename, server_hostname, server_port):
        self.server_hostname = server_hostname
        self.server_port = int(server_port)
        self.server_socket_file = None
        self.input_filename = filename
        self.win = Toplevel()
        self.win.title('ocvmgui:  stitching parameters for %s' % (filename))
        self.stitch_color = StringVar()
        self.stitch_color.set('R')
        group1 = Pmw.Group(self.win, tag_text='(1) Set stitching color')
        group1.pack(side=TOP,fill=X,padx=2)
        self.rgb = Pmw.OptionMenu(group1.interior(), labelpos=W,
                                  label_text='Choose stitching color',
                                  menubutton_textvariable=self.stitch_color,
                                  items=('R','G','B'), menubutton_width=2)
        self.rgb.pack(side=TOP)
        group2 = Pmw.Group(self.win, tag_text='(2) Set cluster host information')
        group2.pack(side=TOP, fill=X, padx=2)
        self.host_scratch = Pmw.ScrolledText(
            group2.interior(), borderframe=1, labelpos=N,
            label_text='', text_wrap='none')
        message = '# Enter host information below in the following format:\n' +\
                  '# \n' + \
                  '# host1 /arbitrary/host1/directory/for/image/output\n' + \
                  '# host2 /some/host2/directory/for/image/output\n' + \
                  '# ...\n'
        self.host_scratch.insert(END, message)
        self.host_scratch.pack(fill=BOTH, expand=1, padx=2, pady=2)
        Button(group2.interior(), text='Import host/scratch file', command=self.import_host_scratch).pack(side=TOP)
        group3 = Pmw.Group(self.win, tag_text='(3) Set output filename')
        group3.pack(side=TOP, expand=NO, fill=X, padx=2, pady=0)
        self.output_filename = Pmw.EntryField(
            group3.interior(), labelpos=W,
            label_text='Output filename (.tif or .dim)',
            value=os.path.splitext(filename)[0] + '.dim')
        self.output_filename.pack(side=TOP, fill=X, padx=5)
        Button(self.win, text='Stitch using these parameters', command=self.stitch).pack(side=TOP)

    def import_host_scratch(self):
        fn = tkFileDialog.askopenfilename(
            parent=self.win, title='Choose a host/scratch file')
        if not fn:
            return
        f = open(fn, "r")
        self.host_scratch.insert(END, f.read())
        f.close()
    def stitch(self):  
        (tempfd, tempfn) = tempfile.mkstemp(suffix=".hs")
        os.write(tempfd, self.host_scratch.get())
        os.close(tempfd)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_hostname, self.server_port))
            self.server_socket_file = sock.makefile()
        except Exception:
            error('problem connecting with server')
            traceback.print_exc()
            return
        self.server_socket_file.write('wummy\n')
        self.server_socket_file.write('stitch\n')
        self.server_socket_file.write(self.input_filename + '\n')
        self.server_socket_file.write(self.output_filename.get() + '\n')
        self.server_socket_file.write(self.stitch_color.get() + '\n')
        self.server_socket_file.write(tempfn + '\n')
        self.server_socket_file.flush()
        t = stitch_img_dialog_completion_waiter(
            self, self.input_filename, self.output_filename.get(), tempfn)
        t.start()
        self.win.destroy()
        notice('Stitching is proceeding on the server.  ' +
               'You will be notified when it completes.')
        
class ImageFrame:
    def __init__(self, file_opener_class,
                 root, filename, server_hostname, server_port, cache_dir):
        self.file_opener_class = file_opener_class
        self.startup_failed = 0
        f = open(filename, "r")
        self.file_contents = f.read()
        f.close()
        sha1 = sha.new()
        sha1.update(self.file_contents)
        self.file_contents_sha1 = sha1.hexdigest()

        (self.image_type, self.pixels_x, self.pixels_y, self.pixels_z,
         self.chunks_x, self.chunks_y, self.chunks_z, self.extra) = \
         self.get_image_info()
        self.associated_prefix_sum_filename = None
        self.associated_prefix_sum_thresholds = None
        if self.extra:
            associated_prefix_sum_filename_orig = self.extra.split()[1]
            self.associated_prefix_sum_filename = \
                associated_prefix_sum_filename_orig
            if not os.path.exists(self.associated_prefix_sum_filename) and \
                   self.associated_prefix_sum_filename[0] != os.path.sep:
                self.associated_prefix_sum_filename = os.path.join(filename, os.path.basename(self.associated_prefix_sum_filename))
            if not os.path.exists(self.associated_prefix_sum_filename):
                error('could not find prefix sum filename', associated_prefix_sum_filename_orig)
                self.startup_failed = 1
                return
            self.associated_prefix_sum_thresholds = \
                [int(x) for x in self.extra.split()[3].split(',')]
            
        self.filename = filename
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.cache_dir = cache_dir
        self.server_socket_file = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_hostname, self.server_port))
            self.server_socket_file = sock.makefile("r+")
            self.server_socket_file.write('wummy\n')
        except Exception:
            error('problem connecting with server')
            traceback.print_exc()
            self.startup_failed = 1
            return

        screenwidth = root.winfo_screenwidth()
        screenheight = root.winfo_screenheight()
        self.used_width = int(screenwidth*0.8)
        self.used_height = int(screenheight*0.8)
        self.win = Toplevel()
        self.win.title('ocvmgui on %s:  %s' % (socket.gethostname(), filename))
        self.win.resizable(0,0)
        self.win.geometry('%dx%d' % (self.used_width, self.used_height))
        self.win.protocol("WM_DELETE_WINDOW", self.exit)
        self.frametop = Frame(self.win,border=1,relief='raised')
        self.image_canvas = Canvas(self.frametop)
        self.image_canvas.bind('<ButtonPress-1>', self.mouse1_down_cond)
        self.image_canvas.bind('<ButtonRelease-1>', self.mouse1_up_cond)
        self.image_canvas.bind('<ButtonPress-2>', self.recenter)
        self.image_canvas.bind('<ButtonPress-3>', self.zoom_out_mouse2)
        self.image_canvas.bind('<Enter>', self.canvas_enter)
        self.image_canvas.bind('<Leave>', self.canvas_leave)

        action_vert_frame = Frame(self.win)
        action_vert_frame.pack(side=TOP, expand=NO, fill=NONE, padx=0, pady=0)
        action_frame = Frame(action_vert_frame)
        action_frame.pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)
        self.mouse_chooser = \
            Pmw.RadioSelect(action_frame,
                            buttontype = 'radiobutton',
                            orient = 'horizontal',
                            labelpos = 'w',
                            command = self.mouse_chooser_callback,
                            label_text = 'Left Mouse',
                            hull_borderwidth = 1,
                            hull_relief = 'ridge')
        self.mouse_chooser.pack(side=LEFT, expand=NO, fill=NONE, padx=0, pady=0)
        self.mouse_chooser.add('Zoom')
        self.mouse_chooser.add('Query')
        self.mouse_chooser.setvalue('Zoom')
        Button(action_frame, text='Compute Prefix Sum', command=self.compute_prefix_sum).pack(side=LEFT, expand=NO, fill=NONE, padx=0, pady=0)
        self.change_z_counter = Pmw.Counter(action_frame,
                                            labelpos=W, label_text='Change Z',
                                            entry_width = 2,
                                            entryfield_value=0,
                                            datatype = \
                                            {'counter' : self.change_z})
        self.change_z_counter.pack(side=LEFT)

        threshold_vert_frame = Frame(self.win)
        threshold_vert_frame.pack(side=TOP)
        threshold_frame = Frame(threshold_vert_frame)
        threshold_frame.pack(side=LEFT, expand=YES, fill=X, padx=0, pady=0)
        self.redcounter = Pmw.Counter(threshold_frame,
                labelpos = 'w',
                label_text = 'R',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = 255,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.redcounter.pack(side=LEFT, expand=YES)
        self.greencounter = Pmw.Counter(threshold_frame,
                labelpos = 'w',
                label_text = 'G',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = 255,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.greencounter.pack(side=LEFT, expand=YES)
        self.bluecounter = Pmw.Counter(threshold_frame,
                labelpos = 'w',
                label_text = 'B',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = 255,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.bluecounter.pack(side=LEFT, expand=YES)
        self.apply_threshold_button = Button(threshold_frame, command=self.apply_threshold, text='Apply Threshold')
        self.disable_threshold_button = Button(threshold_frame, command=self.disable_threshold, text='Disable Threshold')
        self.apply_threshold_button.config(font=('Times',12,'bold'))
        self.disable_threshold_button.config(font=('Times',12,'bold'))
        self.disable_threshold_button.config(state=DISABLED)
        self.apply_threshold_button.pack(side=LEFT, expand=YES)
        self.disable_threshold_button.pack(side=LEFT, expand=YES)
        self.threshold_active = 0

        zoom_vert_frame = Frame(self.win)
        zoom_vert_frame.pack(side=TOP)
        zoom_buttons_frame = Frame(zoom_vert_frame)
        Button(zoom_buttons_frame, command=self.zoom_in_completely,text='zoom in completely').pack(side=LEFT, expand=YES, fill=X)
        Button(zoom_buttons_frame, command=self.zoom_in,text='zoom in').pack(side=LEFT, expand=YES, fill=X)
        self.zoom_factor_counter = Pmw.Counter(
            zoom_buttons_frame,
            entry_width = 2,
            entryfield_value=1,
            entryfield_validate = {'validator' : 'integer',
                                   'min' : 1, 'max' : 65535})
        self.zoom_factor_counter.pack(side=LEFT,expand=YES)
        balloon = Pmw.Balloon(self.win)
        balloon.bind(self.zoom_factor_counter.component('uparrow'), 'increase zoom factor')
        balloon.bind(self.zoom_factor_counter.component('downarrow'), 'decrease zoom factor')
        Button(zoom_buttons_frame, command=self.zoom_out,text='zoom out').pack(side=LEFT, expand=YES, fill=X)
        Button(zoom_buttons_frame, command=self.show_initial_image,text='zoom out completely').pack(side=LEFT, expand=YES, fill=X)
        zoom_buttons_frame.pack(side=LEFT, expand=YES, fill=X)

        self.image_canvas.pack(fill=BOTH, expand=YES)
        self.frametop.pack(fill=BOTH,expand=YES,side=TOP)

        # status bar
        status_bar_frame = Frame(self.win)
        status_bar_frame.pack(side=TOP, expand=NO, fill=NONE, padx=0, pady=0)
        self.status_bar_var = StringVar()
        status_bar_label = Label(status_bar_frame, textvariable=self.status_bar_var)
        status_bar_label.pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)
        
        self.displayed_bbox = None
        self.displayed_reduction_factor = None
        self.displayed_zslice = None
        self.displayed_canvas_offsets = None
        self.displayed_canvas_dimensions = None
        self.maximum_reduction_factor = None
        self.secondmost_maximum_reduction_factor = None
        self.last_returned_data = None
        self.last_returned_data_dimensions = None
        self.mouse1_mode = 'Zoom'
        self.query_startpoint = None
        self.query_endpoint = None
        self.query_box = None

        self.win.update()
        self.update_canvas_widths()
        self.show_initial_image()

    def mouse_chooser_callback(self, tag):
        if tag == 'Query' and not self.associated_prefix_sum_filename:
            error('Cannot query, no prefix sum was built yet.')
            self.mouse_chooser.setvalue('Zoom')
        else:
            if self.mouse1_mode != tag:
                self.status_bar_var.set('')
                if tag == 'Query':
                    self.bluecounter.setentry(
                        `self.associated_prefix_sum_thresholds[0]`)
                    self.greencounter.setentry(
                        `self.associated_prefix_sum_thresholds[1]`)
                    self.redcounter.setentry(
                        `self.associated_prefix_sum_thresholds[2]`)
                    if not self.threshold_active:
                        self.threshold_active = 1
                        self.apply_threshold_button.config(state=DISABLED)
                        self.disable_threshold_button.config(state=DISABLED)
#                         self.bluecounter._counterEntry.config(state=DISABLED)
                    self.refresh()
                else:
                    self.threshold_active = 0
                    self.apply_threshold_button.config(state=NORMAL)
                    self.disable_threshold_button.config(state=DISABLED)
                    self.refresh()
                self.mouse1_mode = tag
        
    def update_canvas_widths(self):
#         self.win.update_idletasks()
        self.canvas_width = self.image_canvas.winfo_width()
        self.canvas_height = self.image_canvas.winfo_height()
        print 'canvas dimensions:', self.canvas_width, self.canvas_height

    # return tuple of: (type,
    #                   pixels_x, pixels_y, pixels_z,
    #                   chunks_x, chunks_y, chunks_z, extra)
    def get_image_info(self):
        lines = self.file_contents.split('\n')
        hash = {}
        for l in lines:
            l = l.strip()
            if len(l)>0:
                k,v = l.split(' ',1)
                hash[k] = v
        return tuple([hash['type']] + \
                     map(lambda x: long(hash[x]),
                         ['pixels_x','pixels_y','pixels_z',
                          'chunks_x','chunks_y','chunks_z']) + \
                     (hash.has_key('extra') and [hash['extra']] or ['']))
    
    def show_initial_image(self):
        w = self.canvas_width
        h = self.canvas_height
        reduction_factor = 1
        while w < self.pixels_x or h < self.pixels_y:
            w += self.canvas_width
            h += self.canvas_height
            reduction_factor = reduction_factor + 1
        self.maximum_reduction_factor = reduction_factor

        fn = os.path.join(self.cache_dir, self.file_contents_sha1)
        fninfo = fn + '.info'
        cache_hit = 0
        if not self.threshold_active and os.path.exists(fn) and \
               os.path.exists(fninfo):
            f = open(fninfo, "r")
            line = f.readline()
            f.close()
            line_split = line.split()
            (canwidth, canheight, pixels_new_image_x, pixels_new_image_y,
             self.displayed_reduction_factor) = \
             map(lambda x: int(x), [line_split[1],line_split[3],line_split[5],line_split[7],line_split[9]])
            if canwidth == self.canvas_width and \
                   canheight == self.canvas_height:
                cache_hit = 1
        if cache_hit:
            # cache hit
            self.server_socket_file.write('dcstartup\n')
            self.server_socket_file.write('bytes ' + `len(self.file_contents)` + '\n')
            self.server_socket_file.write(self.file_contents)
            self.server_socket_file.flush()
            f = open(fn, "r")
            data = f.read()
            f.close()
            f = open(fninfo, "r")
            line = f.readline().strip()
            line_split = line.split()
            (canwidth, canheight, pixels_new_image_x, pixels_new_image_y,
             self.displayed_reduction_factor) = \
             map(lambda x: int(x), [line_split[1],line_split[3],line_split[5],line_split[7],line_split[9]])
            f.close()
            self.image_master = Image.fromstring(
                'RGB', (pixels_new_image_x,pixels_new_image_y), data)
            self.displayed_zslice = 0
            self.displayed_bbox = (0, 0, self.pixels_x-1, self.pixels_y-1)
            self.create_or_recreate_image()
        else:
            self.request_bbox(0, 0, self.pixels_x-1, self.pixels_y-1,
                              0, reduction_factor)
            if not self.last_returned_data:
                self.win.destroy()
                return

            if not self.threshold_active:
                f = open(fn, "w")
                f.write(self.last_returned_data)
                f.close()
                f = open(fn + '.info', "w")
                f.write('canvas_width %d canvas_height %d width %d height %d reduction_factor %d\n' %
                        (self.canvas_width,
                         self.canvas_height,
                         self.last_returned_data_dimensions[0],
                         self.last_returned_data_dimensions[1],
                         self.displayed_reduction_factor))
                f.close()
            
        if reduction_factor == 1:
            self.secondmost_maximum_reduction_factor = 1
        else:
            factor = reduction_factor - 1
            while 1:
                new_width = self.canvas_width * factor
                new_height = self.canvas_height * factor
                if new_width > self.pixels_x or new_height > self.pixels_y:
                    factor = factor - 1
                else:
                    break
            self.secondmost_maximum_reduction_factor = factor
        
    def request_bbox(self,
                     upper_left_x, upper_left_y,
                     lower_right_x, lower_right_y,
                     z_slice, reduction_factor):
        self.last_returned_data = None
        print 'request_bbox', upper_left_x, upper_left_y, lower_right_x, lower_right_y, z_slice, reduction_factor
        print 'width / reduction_factor: %f' % (float(lower_right_x - upper_left_x + 1) / float(reduction_factor))
        print 'height / reduction_factor: %f' % (float(lower_right_y - upper_left_y + 1) / float(reduction_factor))
        self.displayed_bbox = (upper_left_x, upper_left_y,
                               lower_right_x, lower_right_y)
        self.displayed_reduction_factor = reduction_factor
        self.displayed_zslice = z_slice

        try:
            self.server_socket_file.write('fetch\n')
            self.server_socket_file.write('bytes ' + `len(self.file_contents)` + '\n')
            self.server_socket_file.write(self.file_contents)
            self.server_socket_file.write('viewport_dimensions %d %d\n' %
                                          (self.canvas_width,
                                           self.canvas_height))
            self.server_socket_file.write('viewport_bbox %d %d %d %d %d %d\n' %
                                          (upper_left_x, upper_left_y,
                                           lower_right_x, lower_right_y,
                                           z_slice, reduction_factor))
            if self.threshold_active:
                self.server_socket_file.write('threshold r %d g %d b %d\n' %
                                              (int(self.redcounter.get()),
                                               int(self.greencounter.get()),
                                               int(self.bluecounter.get())))
            else:
                self.server_socket_file.write('threshold r -1 g -1 b -1\n')
            self.server_socket_file.flush()
            validation = self.server_socket_file.readline().strip()
            if validation == 'validated 0':
                error_size = self.server_socket_file.readline().strip()
                error_size = int(error_size.split(' ')[1])
                errors = self.server_socket_file.read(error_size)
                error(errors)
                return
            new_image_dimensions = self.server_socket_file.readline().strip()
            print 'new_image_dimensions: ', new_image_dimensions
            (pixels_new_image_x,pixels_new_image_y) = map(lambda x: int(x), new_image_dimensions.split()[1:])
            data = self.server_socket_file.read(3*pixels_new_image_x*pixels_new_image_y)
            self.image_master = Image.fromstring(
                'RGB', (pixels_new_image_x,pixels_new_image_y), data)
            self.last_returned_data = data
            self.last_returned_data_dimensions = (pixels_new_image_x,
                                                  pixels_new_image_y)
    
            self.create_or_recreate_image()
        except Exception:
            error('problem communicating with server')
            traceback.print_exc()
            
    def create_or_recreate_image(self):
        self.image_canvas.delete('all')
        print 'image_master size:', self.image_master.size[0], \
              self.image_master.size[1]
        if self.image_master.size[0] > self.canvas_width:
            print self.image_master.size[0], self.canvas_width
            assert 0
        if self.image_master.size[1] > self.canvas_height:
            print self.image_master.size[1], self.canvas_height
            assert 0
        if self.canvas_width > self.image_master.size[0] or \
               self.canvas_height > self.image_master.size[1]:
            self.image_smaller_than_canvas = 1
        else:
            self.image_smaller_than_canvas = 0
        self.photo_master = ImageTk.PhotoImage(self.image_master)
        x_offset = (self.canvas_width - self.image_master.size[0]) / 2
        y_offset = (self.canvas_height - self.image_master.size[1]) / 2
        self.image_canvas.create_image(x_offset, y_offset,
                                       image=self.photo_master, anchor=NW)
        self.displayed_canvas_offsets = (x_offset, y_offset)
        self.displayed_canvas_dimensions = self.image_master.size
    
    def scroll_up(self):
        if self.image_smaller_than_canvas:
            return
        bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        new_ul_x = self.displayed_bbox[0]
        new_ul_y = self.displayed_bbox[1] - bbox_height
        new_lr_x = self.displayed_bbox[2]
        new_lr_y = self.displayed_bbox[3] - bbox_height
        if new_ul_y < 0:
            new_lr_y = new_lr_y - new_ul_y
            new_ul_y = 0
            
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           self.displayed_reduction_factor)

    def scroll_down(self):
        if self.image_smaller_than_canvas:
            return
        bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        new_ul_x = self.displayed_bbox[0]
        new_ul_y = self.displayed_bbox[1] + bbox_height
        new_lr_x = self.displayed_bbox[2]
        new_lr_y = self.displayed_bbox[3] + bbox_height
        if new_lr_y >= self.pixels_y:
            new_ul_y = self.pixels_y - bbox_height
            new_lr_y = self.pixels_y - 1
            
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           self.displayed_reduction_factor)

    def scroll_left(self):
        if self.image_smaller_than_canvas:
            return
        bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        new_ul_x = self.displayed_bbox[0] - bbox_width
        new_ul_y = self.displayed_bbox[1]
        new_lr_x = self.displayed_bbox[2] - bbox_width
        new_lr_y = self.displayed_bbox[3]
        if new_ul_x < 0:
            new_ul_x = 0
            new_lr_x = bbox_width - 1
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           self.displayed_reduction_factor)

    def scroll_right(self):
        if self.image_smaller_than_canvas:
            return
        bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        new_ul_x = self.displayed_bbox[0] + bbox_width
        new_ul_y = self.displayed_bbox[1]
        new_lr_x = self.displayed_bbox[2] + bbox_width
        new_lr_y = self.displayed_bbox[3]
        if new_lr_x >= self.pixels_x:
            new_ul_x = self.pixels_x - bbox_width
            new_lr_x = self.pixels_x - 1
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           self.displayed_reduction_factor)

    def locus_to_upper_left(self, locus, new_width, new_height):
        new_ul_x = locus[0] - new_width/2
        if new_ul_x < 0:
            new_ul_x = 0
        elif new_ul_x + new_width >= self.pixels_x:
            new_ul_x = self.pixels_x - new_width - 1
        else:
            assert new_ul_x < self.pixels_x

        new_ul_y = locus[1] - new_height/2
        if new_ul_y < 0:
            new_ul_y = 0
        elif new_ul_y + new_height >= self.pixels_y:
            new_ul_y = self.pixels_y - new_height - 1
        else:
            assert new_ul_y < self.pixels_y
        return (new_ul_x, new_ul_y)
           
    def zoom_in(self, locus=None):
        new_ul_x = None
        new_ul_y = None
        new_lr_x = None
        new_lr_y = None
        factor = self.displayed_reduction_factor
        if factor == 1:
            print 'cannot zoom in any further'
            return
        elif self.image_smaller_than_canvas: # zoom into initially
                                             # placed thumnail
            factor = max(1, factor - int(self.zoom_factor_counter.get()))
            print 'new factor:', factor
            while 1:
                new_width = self.canvas_width * factor
                new_height = self.canvas_height * factor
                if new_width > self.pixels_x or new_height > self.pixels_y:
                    factor = factor - 1
                else:
                    break
            out_of_view_x = self.pixels_x - new_width
            out_of_view_y = self.pixels_y - new_height
            if locus:
                new_ul_x, new_ul_y = self.locus_to_upper_left(
                    locus, new_width, new_height)
            else:
                new_ul_x = out_of_view_x / 2
                new_ul_y = out_of_view_y / 2
            new_lr_x = new_ul_x + new_width - 1
            new_lr_y = new_ul_y + new_height - 1
        else:
            old_bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
            old_bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
            assert (old_bbox_width % factor) == 0
            assert (old_bbox_height % factor) == 0
            factor_change = int(self.zoom_factor_counter.get())
            if factor_change >= factor:
                factor_change = factor - 1
            print 'factor_change:', factor_change
            reduction_width = (old_bbox_width / factor) * factor_change
            reduction_height = (old_bbox_height / factor) * factor_change
            new_width = old_bbox_width - reduction_width
            new_height = old_bbox_height - reduction_height
            if locus:
                new_ul_x, new_ul_y = self.locus_to_upper_left(
                    locus, new_width, new_height)
            else:
                new_ul_x = self.displayed_bbox[0] + reduction_width/2
                new_ul_y = self.displayed_bbox[1] + reduction_height/2
            new_lr_x = new_ul_x + new_width - 1
            new_lr_y = new_ul_y + new_height - 1
            factor = factor - factor_change
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           factor)
    def zoom_in_completely(self, locus=None):
        new_ul_x = None
        new_ul_y = None
        new_lr_x = None
        new_lr_y = None
        factor = self.displayed_reduction_factor
        if factor == 1:
            print 'cannot zoom in any further'
            return
        elif self.image_smaller_than_canvas and \
                 (self.canvas_width > self.pixels_x or \
                  self.canvas_height > self.pixels_y):
            error('cannot zoom, image is smaller than window')
            return
        old_bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        old_bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        new_width = self.canvas_width
        new_height = self.canvas_height
        new_ul_x = ((self.displayed_bbox[0] + self.displayed_bbox[2]) / 2) - \
                   (new_width / 2)
        new_ul_y = ((self.displayed_bbox[1] + self.displayed_bbox[3]) / 2) - \
                   (new_height / 2)
        new_lr_x = new_ul_x + new_width - 1
        new_lr_y = new_ul_y + new_height - 1
        factor = 1
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           factor)
    def zoom_out(self, locus=None):
        factor = self.displayed_reduction_factor
        factor_increase = int(self.zoom_factor_counter.get())
        if factor == self.maximum_reduction_factor:
            print 'Cannot zoom out any further'
            return
        elif factor == self.secondmost_maximum_reduction_factor or \
             factor + factor_increase > \
             self.secondmost_maximum_reduction_factor:
            return self.show_initial_image()
        old_bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        old_bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1
        assert (old_bbox_width % factor) == 0
        assert (old_bbox_height % factor) == 0
        increase_width = self.canvas_width * factor_increase
        increase_height = self.canvas_height * factor_increase
        new_width = old_bbox_width + increase_width
        new_height = old_bbox_height + increase_height
        if locus:
            new_ul_x,new_ul_y = self.locus_to_upper_left(locus,
                                                         new_width,
                                                         new_height)
        else:
            new_ul_x = ((self.displayed_bbox[2] + self.displayed_bbox[0])/2) - \
                       (new_width/2)
            new_ul_y = ((self.displayed_bbox[3] + self.displayed_bbox[1])/2) - \
                       (new_height/2)
        new_lr_x = new_ul_x + new_width - 1
        new_lr_y = new_ul_y + new_height - 1
        if new_ul_x < 0:
            new_lr_x = new_lr_x - new_ul_x
            new_ul_x = 0
        elif new_lr_x >= self.pixels_x:
            new_ul_x = new_ul_x - (new_lr_x - (self.pixels_x-1))
            new_lr_x = self.pixels_x - 1
        if new_ul_y < 0:
            new_lr_y = new_lr_y - new_ul_y
            new_ul_y = 0
        elif new_lr_y >= self.pixels_y:
            new_ul_y = new_ul_y - (new_lr_y - (self.pixels_y-1))
            new_lr_y = self.pixels_y - 1
        factor = factor + factor_increase
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           self.displayed_zslice,
                           factor)
        
    def mouse1_down_cond(self, event):
        if self.mouse1_mode == 'Zoom':
            self.zoom_in_mouse1(event)
        else:
            self.query_mouse1_down(event)

    def mouse1_up_cond(self, event):
        if self.mouse1_mode == 'Zoom':
            pass
        else:
            self.query_mouse1_up(event)

    def zoom_in_mouse1(self, event):
        print event.x, event.y
        canvasx, canvasy = (self.image_canvas.canvasx(event.x), \
                            self.image_canvas.canvasy(event.y))
        print canvasx, canvasy
        offsets = self.displayed_canvas_offsets
        dims = self.displayed_canvas_dimensions
        if canvasx < offsets[0] or canvasy < offsets[1] or \
           canvasx >= offsets[0] + dims[0] or \
           canvasy >= offsets[1] + dims[1]:
            return
        x_pct, y_pct = self.zoom_mouse_calc_percentages(canvasx, canvasy)
        self.zoom_in((int(x_pct * self.pixels_x),
                      int(y_pct * self.pixels_y)))

    def zoom_out_mouse2(self, event):
        print event.x, event.y
        canvasx, canvasy = (self.image_canvas.canvasx(event.x), \
                            self.image_canvas.canvasy(event.y))
        print canvasx, canvasy
        offsets = self.displayed_canvas_offsets
        dims = self.displayed_canvas_dimensions
        if canvasx < offsets[0] or canvasy < offsets[1] or \
           canvasx >= offsets[0] + dims[0] or \
           canvasy >= offsets[1] + dims[1]:
            return
        x_pct, y_pct = self.zoom_mouse_calc_percentages(canvasx, canvasy)
        self.zoom_out((int(x_pct * self.pixels_x),
                       int(y_pct * self.pixels_y)))

    def zoom_mouse_calc_percentages(self, canvasx, canvasy):
        offsets = self.displayed_canvas_offsets
        dims = self.displayed_canvas_dimensions
        if self.image_smaller_than_canvas: # zoom into initial image
            x_pct = (canvasx - offsets[0]) / dims[0]
            y_pct = (canvasy - offsets[1]) / dims[1]
        else:
            x_pct = (self.displayed_bbox[0] + \
                     (canvasx * ((self.displayed_bbox[2] -
                                  self.displayed_bbox[0] + 1) / dims[0]))) / \
                                  self.pixels_x
            y_pct = (self.displayed_bbox[1] + \
                     (canvasy * ((self.displayed_bbox[3] -
                                  self.displayed_bbox[1] + 1) / dims[1]))) / \
                                  self.pixels_y
        return (x_pct, y_pct)

    def recenter(self, event):
        print event.x, event.y
        canvasx, canvasy = (self.image_canvas.canvasx(event.x), \
                            self.image_canvas.canvasy(event.y))
        print canvasx, canvasy
        offsets = self.displayed_canvas_offsets
        dims = self.displayed_canvas_dimensions
        if canvasx < offsets[0] or canvasy < offsets[1] or \
           canvasx >= offsets[0] + dims[0] or \
           canvasy >= offsets[1] + dims[1]:
            return
        x_pct, y_pct = self.zoom_mouse_calc_percentages(canvasx, canvasy)
        new_center_x = int(x_pct * self.pixels_x)
        new_center_y = int(y_pct * self.pixels_y)

        old_bbox_width = self.displayed_bbox[2] - self.displayed_bbox[0] + 1
        old_bbox_height = self.displayed_bbox[3] - self.displayed_bbox[1] + 1

        new_ul_x,new_ul_y = self.locus_to_upper_left((new_center_x,
                                                      new_center_y),
                                                     old_bbox_width,
                                                     old_bbox_height)
        new_lr_x = new_ul_x + old_bbox_width-1
        new_lr_y = new_ul_y + old_bbox_height-1
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                          self.displayed_zslice,
                          self.displayed_reduction_factor)

    def change_z(self, text, factor, increment):
        print text, factor, increment
        current = int(text) + factor
        if current == self.chunks_z or current == -1:
            raise ValueError
        print 'new z is', current

        new_ul_x = self.displayed_bbox[0]
        new_ul_y = self.displayed_bbox[1]
        new_lr_x = self.displayed_bbox[2]
        new_lr_y = self.displayed_bbox[3]
        self.request_bbox(new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                          current,
                          self.displayed_reduction_factor)

        return `current`

    def canvas_enter(self, event):
        if self.mouse1_mode == 'Query':
            self.win.config(cursor='cross')

    def canvas_leave(self, event):
        self.win.config(cursor='')

    def refresh(self):
        bbox = self.displayed_bbox
        self.request_bbox(bbox[0], bbox[1], bbox[2], bbox[3],
                          self.displayed_zslice,
                          self.displayed_reduction_factor)

    def apply_threshold(self):
        if not self.threshold_active:
            self.threshold_active = 1
            self.disable_threshold_button.config(state=NORMAL)
        self.refresh()

    def disable_threshold(self):
        if self.threshold_active:
            self.threshold_active = 0
            self.disable_threshold_button.config(state=DISABLED)
        self.refresh()

    def compute_prefix_sum(self):
        d = compute_prefix_sum_dialog(
            self,
            self.filename,
            self.server_hostname,
            self.server_port,
            int(self.redcounter.get()),
            int(self.greencounter.get()),
            int(self.bluecounter.get()))

    def query_mouse1_down(self, event):
        x, y = self.image_canvas.canvasx(event.x), \
               self.image_canvas.canvasy(event.y)
        self.query_startpoint = [x,y]
        self.image_canvas.bind('<Motion>', self.query_motion)

    def query_motion(self, event):
        x, y = self.image_canvas.canvasx(event.x), \
               self.image_canvas.canvasy(event.y)
        if self.query_box:
            self.image_canvas.delete(self.query_box)
        self.query_box = self.image_canvas.create_rectangle(
            self.query_startpoint[0],
            self.query_startpoint[1],
            x, y, outline='white')

    def query_mouse1_up(self, event):
        if self.query_box:
            self.image_canvas.delete(self.query_box)
            self.query_box = None
        x, y = self.image_canvas.canvasx(event.x), \
               self.image_canvas.canvasy(event.y)
        self.query_endpoint = [x, y]

        width_q = abs(self.query_startpoint[0] - self.query_endpoint[0])
        height_q = abs(self.query_startpoint[1] - self.query_endpoint[1])
        min_x = min(self.query_endpoint[0], self.query_startpoint[0])
        min_y = min(self.query_endpoint[1], self.query_startpoint[1])
        max_x = max(self.query_endpoint[0], self.query_startpoint[0])
        max_y = max(self.query_endpoint[1], self.query_startpoint[1])

        self.query_startpoint = [min_x, min_y]
        self.query_endpoint =   [max_x, max_y]
        assert width_q == self.query_endpoint[0] - self.query_startpoint[0] + 1
        assert height_q == self.query_endpoint[1] - self.query_startpoint[1] + 1

        print 'query is from', self.query_startpoint, 'to', self.query_endpoint
        self.image_canvas.unbind('<Motion>')

        query_width = self.query_endpoint[0] - self.query_startpoint[0] + 1
        query_height = self.query_endpoint[1] - self.query_startpoint[1] + 1
        factor = self.displayed_reduction_factor
        if self.image_smaller_than_canvas:
            left_gap = (self.canvas_width -
                         self.displayed_canvas_dimensions[0]) / 2
            above_gap = (self.canvas_height -
                         self.displayed_canvas_dimensions[1]) / 2
            print 'left_gap', left_gap
            print 'above_gap', above_gap
            actual_ul_x = (self.query_startpoint[0] - left_gap) * factor
            actual_ul_y = (self.query_startpoint[1] - above_gap) * factor
        else:
            actual_ul_x = self.displayed_bbox[0] + \
                          (self.query_startpoint[0] * factor)
            actual_ul_y = self.displayed_bbox[1] + \
                          (self.query_startpoint[1] * factor)
        actual_lr_x = actual_ul_x + (query_width*factor) - 1
        actual_lr_y = actual_ul_y + (query_height*factor) - 1

        print 'actual query box:', actual_ul_x, actual_ul_y, 'to', \
              actual_lr_x, actual_lr_y
        self.server_socket_file.write('psquery\n')
        self.server_socket_file.write('bytes ' + `len(self.file_contents)` + '\n')
        self.server_socket_file.write(self.file_contents)
        print 'extra is', self.extra
        (d1, prefix_sum_descriptor_fn, d2, thresholds_bgr) = \
             self.extra.split()
        data = open(prefix_sum_descriptor_fn).read()
        self.server_socket_file.write('bytes ' + `len(data)` + '\n')
        self.server_socket_file.write(data)
        self.server_socket_file.write(`actual_ul_x` + '\n')
        self.server_socket_file.write(`actual_ul_y` + '\n')
        self.server_socket_file.write(`actual_lr_x` + '\n')
        self.server_socket_file.write(`actual_lr_y` + '\n')
        self.server_socket_file.write(`self.displayed_zslice` + '\n')
        self.server_socket_file.flush()
        line = self.server_socket_file.readline().strip()
        vals = line.split()[1:]
        print 'vals are', vals
        txt = 'Pixels in query box:' + \
              ' red=' + `vals[2]` + \
              ' green=' + `vals[1]` + \
              ' blue=' + `vals[0]`
        print txt
        self.status_bar_var.set(txt)

    def exit(self):
        if active_image_frames.has_key(self):
            del active_image_frames[self]
            self.server_socket_file.write('close\n')
            self.server_socket_file.close()
            self.win.destroy()

class compute_prefix_sum_dialog:
    def __init__(self, calling_class,
                 filename, server_hostname, server_port,
                 r, g, b):
        self.calling_class = calling_class
        self.filename = filename
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.server_socket_file = None
        self.input_filename = filename
        self.output_filename = None
        self.win = Toplevel()
        self.win.geometry('%dx%d' % (400, 300))
        self.win.title('ocvmgui on %s:  prefix sum parameters for %s' % \
                       (socket.gethostname(), filename))

        lines = open(filename).read().split('\n')
        h = {}
        for l in lines:
            l = l.strip()
            if len(l)>0:
                k,v = l.split(' ',1)
                h[k] = v
        self.pixels_x = long(h['pixels_x'])
        self.pixels_y = long(h['pixels_y'])
        print self.pixels_x, self.pixels_y

        group1 = Pmw.Group(self.win, tag_text='(1) Set threshold parameters')
        group1.pack(side=TOP,fill=X,padx=2)
        Label(group1.interior(), text='').pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)
        self.redcounter = Pmw.Counter(group1.interior(),
                labelpos = 'w',
                label_text = 'R',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = r,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.redcounter.pack(side=LEFT)
        self.greencounter = Pmw.Counter(group1.interior(),
                labelpos = 'w',
                label_text = 'G',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = g,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.greencounter.pack(side=LEFT)
        self.bluecounter = Pmw.Counter(group1.interior(),
                labelpos = 'w',
                label_text = 'B',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = b,
                entryfield_validate = {'validator' : 'integer',
                        'min' : -1, 'max' : 255})
        self.bluecounter.pack(side=LEFT)
        Label(group1.interior(), text='').pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)
        group2 = Pmw.Group(self.win, tag_text='(2) Set tessellation values')
        group2.pack(side=TOP, fill=X, padx=2, pady=3)
        Label(group2.interior(), text='').pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)
        self.xcounter = Pmw.Counter(group2.interior(),
                labelpos = 'n',
                label_text = 'X',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = 4,
                entryfield_validate = {'validator' : 'integer',
                                       'min' : 1, 'max' : 999})
        self.xcounter.pack(side=LEFT, expand=NO, fill=NONE, padx=0, pady=0)
        byframe = Frame(group2.interior())
        byframe.pack(side=LEFT, expand=NO, fill=NONE, padx=2, pady=0)
        Label(byframe, text='').pack(side=TOP, expand=YES, fill=NONE, padx=0, pady=0)
        Label(byframe, text='x').pack(side=TOP, expand=NO, fill=NONE, padx=0, pady=0)
        self.ycounter = Pmw.Counter(group2.interior(),
                labelpos = 'n',
                label_text = 'Y',
                orient = 'horizontal',
                entry_width = 3,
                entryfield_value = 4,
                entryfield_validate = {'validator' : 'integer',
                                       'min' : 1, 'max' : 999})
        self.ycounter.pack(side=LEFT, expand=NO, fill=NONE, padx=0, pady=0)
        Label(group2.interior(), text='').pack(side=LEFT, expand=YES, fill=NONE, padx=0, pady=0)

        group3 = Pmw.Group(self.win, tag_text='(3) Set main memory to use per node')
        group3.pack(side=TOP, expand=NO, fill=X, padx=2, pady=3)
        self.memory_amount = Pmw.EntryField(
            group3.interior(), labelpos=W,
            label_text='Memory to use per node',
            value='512m',
            entry_width=10)
        self.memory_amount.pack(side=TOP, fill=X, padx=5)

        group4 = Pmw.Group(self.win, tag_text='(4) Set output filename')
        group4.pack(side=TOP, expand=NO, fill=X, padx=2, pady=3)
        self.output_filename = Pmw.EntryField(
            group4.interior(), labelpos=W,
            label_text='Output filename (.dim)',
            value=os.path.basename(os.path.splitext(filename)[0] + '-ps.dim'))
        self.output_filename.pack(side=TOP, fill=X, padx=5)
        Button(self.win, text='Compute Prefix Sum', command=self.compute_it).pack(side=TOP)

    def compute_it(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_hostname, self.server_port))
            self.server_socket_file = sock.makefile()
        except Exception:
            error('problem connecting with server')
            traceback.print_exc()
            return
        self.server_socket_file.write('wummy\n')
        self.server_socket_file.write('psum\n')
        self.server_socket_file.write(self.redcounter.get() + '\n')
        self.server_socket_file.write(self.greencounter.get() + '\n')
        self.server_socket_file.write(self.bluecounter.get() + '\n')
        self.server_socket_file.write(self.xcounter.get() + '\n')
        self.server_socket_file.write(self.ycounter.get() + '\n')
        self.server_socket_file.write(self.memory_amount.get() + '\n')
        input = open(self.input_filename).read()
        self.server_socket_file.write('bytes %d\n' % len(input))
        self.server_socket_file.write(input)
        self.server_socket_file.write(self.output_filename.get() + '\n')

        self.server_socket_file.flush()
        t = compute_prefix_sum_dialog_completion_waiter(
            self.calling_class,
            self, self.input_filename, self.output_filename.get())
        t.start()
        self.win.destroy()
        notice('Prefix sum generation is proceeding on the server.  ' +
               'You will be notified when it completes.')

class compute_prefix_sum_dialog_completion_waiter(threading.Thread):
    def __init__(self, image_frame_class,
                 compute_prefix_sum_dialog_class, original_filename,
                 output_filename):
        threading.Thread.__init__(self)
        self.image_frame_class = image_frame_class
        self.compute_prefix_sum_dialog_class = compute_prefix_sum_dialog_class
        self.original_filename = original_filename
        self.output_filename = output_filename
    def run(self):
        line = self.compute_prefix_sum_dialog_class.server_socket_file.readline().strip()
        (header, val) = line.split()
        if val == "0":
            line = self.compute_prefix_sum_dialog_class.server_socket_file.readline().strip()
            (header, bytes) =  line.split()
            new_image_descriptor = self.compute_prefix_sum_dialog_class.server_socket_file.read(int(bytes))
            open(self.original_filename, "w").write(new_image_descriptor)
            line = self.compute_prefix_sum_dialog_class.server_socket_file.readline().strip()
            (header, bytes) = line.split()
            new_prefix_sum_descriptor = self.compute_prefix_sum_dialog_class.server_socket_file.read(int(bytes))
            open(os.path.join(os.path.dirname(self.original_filename), self.output_filename), "w").write(new_prefix_sum_descriptor)
        self.compute_prefix_sum_dialog_class.server_socket_file.write('close\n')
        self.compute_prefix_sum_dialog_class.server_socket_file.flush()
        self.compute_prefix_sum_dialog_class.server_socket_file.close()
        pending_tk_events_mutex.acquire()
        if val == "0":
            pending_tk_events.append(
                [notice,
                 'prefix sum generation of file %s to output file %s '
                 'completed cleanly' %
                 (self.original_filename, self.output_filename)])
            pending_tk_events.append(
                [reopen, self.image_frame_class])
        else:
            pending_tk_events.append(
                [error,
                 ('prefix sum generation of file %s returned %s' % \
                  (self.original_filename, val))])
        pending_tk_events_mutex.release()

def reopen(image_frame_class):
    image_frame_class.exit()
    image_frame_class.file_opener_class.load_image_frame(image_frame_class.filename)

def main():
    global command_line_files
    command_line_files = []
    if ((len(sys.argv)-1) > 0):
        command_line_files = map(lambda x: os.path.realpath(x), sys.argv[1:])
    fo = file_opener()
    
if __name__ == "__main__": # when run as a script
    main()


