import os, os.path, string
import sys, commands

def which(filename): 
    if not os.environ.has_key('PATH') or os.environ['PATH'] == '': 
        p = os.defpath 
    else: 
        p = os.environ['PATH'] 
    pathlist = string.split(p, os.pathsep) 
    for path in pathlist: 
        f = os.path.join(path, filename) 
        if os.access(f, os.X_OK): 
            return f 
    return None 

opts = Options('config.py')
opts.AddOptions(
    EnumOption('intrinsics', 'use MMX or SSE intrinsics', 'off',
               allowed_values=('off', 'MMX', 'SSE')),
    ('debug', 'Set to 1 to build a debug build', 0),
    ('debug_flags', 'Set to use these flags during debug build', None),
    ('prefix', 'Set to install prefix', '/usr/local'),
    ('gprof', 'Set to 1 to build with gprof flags', 0),
    ('extralibs', 'List any additional link libraries here', None),
    ('extradefines', 'List any additional preprocessor defines here', None),
    ('use_java', 'Whether to build java filters', 1),
    ('CXX', 'Forces C++ compiler', None),
    ('CXXFLAGS', 'Forces C++ compiler flags', None),
    ('RPATH', ' Forces RPATH', None),
    )

defines = []
libs = []

env = Environment(ENV=os.environ, options=opts)
env.SConsignFile()

Help(opts.GenerateHelpText(env))

intrinsics = env.get('intrinsics')
debug_mode = int(env.get('debug'))
debug_flags = env.get('debug_flags')
install_prefix = env.get('prefix')
gprof_mode = int(env.get('gprof'))
extralibs = env.get('extralibs')
extradefines = env.get('extradefines')
use_java = env.get('use_java')
env['use_java'] = use_java

if which('dcmpideps') == None:
    sys.stderr.write("ERROR:  cannot find executable 'dcmpideps', have you installed dcmpi and ensured\nthe 'bin' directory is in your PATH?  (Or if you are using it without installing\nhave you added the 'src' directory of the build directory to your PATH?)")
    sys.exit(1)

env.ParseConfig('dcmpideps --cflags')
env.ParseConfig('dcmpideps --libs')

env['JAVACFLAGS'] = (debug_mode and "-g " or "") + \
                        "-classpath " + \
                        commands.getoutput("dcmpideps --classpath")
env['JAVACFLAGS'] = env['JAVACFLAGS'] + ':' \
    'deps/ij.jar:deps/jade_1_0.jar:deps/jai_codec.jar:deps/jai_core.jar:deps/mlibwrapper_jai.jar:deps/ncmir_plugins.jar:deps/jargon_v1.4.19.jar'

if intrinsics != 'off':
    print 'using', intrinsics, 'intrinsics'

if extralibs:
    libs.extend(extralibs.split(' '))
if extradefines:
    defines.extend(extradefines.split(' '))

if debug_mode:
    print 'building with debug'
    env['CXXFLAGS'] = env['CXXFLAGS'] + ' -g'
    env.Append(CPPDEFINES=['DEBUG'])
    if debug_flags:
        print 'building with debug flags of', debug_flags
        env['CXXFLAGS'] = env['CXXFLAGS'] + ' ' + debug_flags
else:
    print 'building optimized version'
    env['CXXFLAGS'] = env['CXXFLAGS'] + ' -O3'
    env.Append(CPPDEFINES=['NDEBUG'])

context = env.Configure()

if intrinsics == 'SSE':
    defines.append('OCVM_USE_SSE_INTRINSICS')
    if env['CXX'] == 'g++':
        env['CXXFLAGS'] = env['CXXFLAGS'] + ' -march=pentium4 '
elif intrinsics == 'MMX':
    defines.append('OCVM_USE_MMX_INTRINSICS')
    if env['CXX'] == 'g++':
        env['CXXFLAGS'] = env['CXXFLAGS'] + ' -march=pentium3 '

# for large files support
# see http://www.suse.de/~aj/linux_lfs.html
defines.append('_FILE_OFFSET_BITS=64')
defines.append('_LARGEFILE_SOURCE')
env['CXXCOM'] = string.replace(env['CXXCOM'],'$SOURCES','$SOURCES.abspath')
env['SHCXXCOM'] = string.replace(env['SHCXXCOM'],'$SOURCES','$SOURCES.abspath')

env.Append(CPPDEFINES=defines)
env.Append(CPPPATH=['#/src'])
env.Append(LIBPATH='#/src')
env['install_prefix'] = install_prefix

Export("env")
SConscript('src/SConscript')
SConscript('deps/SConscript')
