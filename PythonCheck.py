from os import path
from shutil import copyfile
import sys
if path.exists(r"C:\Python27\ArcGIS10.8\python32.exe")== False:
    copyfile(r"C:\Python27\ArcGIS10.8\python.exe", r"C:\Python27\ArcGIS10.8\python32.exe")
    print "created 32-bit python"
else:
    print "32-bit python already exists"

raw_input('press enter to exit')
