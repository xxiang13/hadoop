# -*- coding: utf-8 -*-
"""
Created on Sat May  2 19:47:22 2015

@author: apple
"""
import os
import sys

input_hdfs = sys.argv[1]
out_hdfs = sys.argv[2]

num_cluster=int(raw_input('Number of cluster:'))

def initial_centroid(num_cluster): 
    clusterNum = str(num_cluster)
    #os.system("sort -R medicare_standarized.txt | head -n " + clusterNum + "| nl > centroid.txt")
    os.system("shuf -n " + clusterNum + " " + "medicare_standarized.txt | nl > centroid.txt")
    os.system("hadoop fs -rm -r DC")
    os.system("sed -i 's/^ *//g' centroid.txt")

    os.system("hadoop fs -mkdir DC")
    os.system("hadoop fs -put centroid.txt DC")
    
initial_centroid(num_cluster)

for i in range(10):

    os.system("hadoop fs -rm -r "+out_hdfs)
    os.system("hadoop jar XiangLi1_Miller2_exercise.jar "+input_hdfs+" "+ out_hdfs)
    os.system("hadoop fs -getmerge "+ out_hdfs+" centroid.txt")
    #distributed cache file = centroid with initial DC, centroid.txt    
    os.system("hadoop fs -rm -r DC")
    os.system("hadoop fs -mkdir DC")
    os.system("rm .centroid.txt.crc")
    os.system("hadoop fs -put centroid.txt DC")









    

