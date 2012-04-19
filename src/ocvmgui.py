#!/usr/bin/env python

import ocvmguimain, sys

if __name__ == "__main__": # when run as a script
    try:
        ocvmguimain.main()
    except KeyboardInterrupt:
        sys.exit(1)


