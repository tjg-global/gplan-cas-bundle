#!python3
import os, sys
import argparse
import re
import subprocess

import git
print(git.__version__)

def main():
    raise NotImplementedError

def command_line():
    parser = argparse.ArgumentParser()
    #~ parser.add_argument("command")
    args = parser.parse_args()
    main()

if __name__ == '__main__':
    command_line()
