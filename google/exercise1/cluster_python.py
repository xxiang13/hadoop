# -*- coding: utf-8 -*-
"""
Created on Sat May  2 19:47:22 2015

@author: apple
"""
import os
import sys

input_hdfs = sys.argv[1]
out_hdfs = sys.argv[2]


os.system("hadoop fs -rm -r" + out_hdfs)
os.system("hadoop jar XiangLi1.exercise1.jar "+"input_hdfs"+" "+ input_hdfs)
os.system("hadoop fs -getmerge "+ out_hdfs+" result.txt")