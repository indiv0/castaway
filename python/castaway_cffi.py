import cffi
import subprocess


PACKAGE_NAME = "castaway"
TARGET_DIR = "target"
BINDINGS_FILE = "{t}/{n}.h".format(t=TARGET_DIR, n=PACKAGE_NAME)
# TODO: use the --release build
LIBRARY_FILE = "{t}/debug/libcastaway.so".format(t=TARGET_DIR)


def load(filename):
    with open(filename, 'r') as f:
        return f.read()


ffi = cffi.FFI()
ffi.cdef(load(BINDINGS_FILE))

lib = ffi.dlopen(LIBRARY_FILE)
