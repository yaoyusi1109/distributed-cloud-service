# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022

import threading # for getting current thread name

# log(msg) prints a message to standard output. Since multi-threading can jumble
# up the order of output on the screen, we print out the current thread's name
# on each line of output along with the message.
# Example usage:
#   log("Hello %s, you are customer number %d, have a nice day!" % (name, n))
def log(msg):
    # Convert msg to a string, if it is not already
    if not isinstance(msg, str):
        msg = str(msg)
    # Each python thread has a name. Use current thread's in the output message.
    myname = threading.current_thread().name
    # When printing multiple lines, indent each line a bit
    indent = (" " * len(myname))
    linebreak = "\n" + indent + ": "
    lines = msg.splitlines()
    msg = linebreak.join(lines)
    # Print it all out, prefixed by this thread's name.
    print(myname + ": " + msg)

ANSI_BLACK_BG = '\033[40m'
ANSI_RED = '\033[31m'
ANSI_ORANGE = '\033[33m'
ANSI_RESET = '\033[0m'

# logerr(msg) is the same as log(msg), but prints in red.
def logerr(msg):
    log(ANSI_RED + ANSI_BLACK_BG + msg + ANSI_RESET)

# logwarn(msg) is the same as log(msg), but prints in yellow.
def logwarn(msg):
    log(ANSI_ORANGE + ANSI_BLACK_BG + msg + ANSI_RESET)

