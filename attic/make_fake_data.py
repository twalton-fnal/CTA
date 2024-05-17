#!/usr/local/bin python3

import os, sys, string, re, shutil, math, subprocess, json
import matplotlib.pyplot as plt
import matplotlib.ticker as tick

import ROOT

from faker import Faker
import numpy as np
import pandas as pd




print( "Enter creating fake data based on NOvA analysis dataset\n\n")

fake_file_sizes = []
total_file_sizes = 0

# open file and get file sizes
fnova = open('/home/eos/prometheus/analysis/nova.files.txt', 'r')
lnova = fnova.readlines()

for l, lline in enumerate(lnova) :
    if l%25000 == 0 :
       print( "At line %d : [%s]" % (l,lline.strip()) )

    lwords = lline.strip().split("\t")

    fake_file_sizes.append(int(lwords[2]))    
    total_file_sizes += int(lwords[2])

fnova.close()

print( "\nCompleted reading lines [%d], with total files giving [%d] size (byte)" % ( len(lnova),total_file_sizes ) )

file_size_tb = total_file_sizes * 1e-12
print( "\tfile in TB [%.3f]" % (file_size_tb) )




