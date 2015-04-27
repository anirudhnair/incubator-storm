#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
import subprocess

def main(file, cmd):
    print cmd, "writing to", file
    out = open(file, "w")
    count = 0
    process = subprocess.Popen(cmd,
                           stderr=subprocess.STDOUT,
                           stdout=subprocess.PIPE)

    # wait for the process to terminate
    pout = process.stdout
    line = pout.readline()
    while line:
        count = count + 1
        if count > 9:
            sys.stdout.write(".")
            sys.stdout.flush()
            count = 0
        out.write(line)
        line = pout.readline()
    out.close()
    errcode = process.wait()
    print
    print cmd, "done", errcode
    return errcode

if __name__ == "__main__":
    if sys.argv < 1:
        print "Usage: %s [file info]" % sys.argv[0]
        sys.exit(1)

    sys.exit(main(sys.argv[1], sys.argv[2:]))
