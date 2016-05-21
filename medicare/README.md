#Segmentation Analysis with US Government Medicare Data utilizing Hadoop MapReduce

### Implement the Kmeans clustering algorithm 
Assume that: 
1)  The number of clusters is given in advance.  
2)  The input files all reside in one folder. Each record corresponds to a data observation. Each 
column corresponds to a coordinate of the underlying vector. The coordinates are separated by 
comma.  
3)  The output must be a set of centroids.  


As an external ‘tool’ you are free to use the tool of your choice, but all mapreduce routines must be  written in java. 

You have: 
1)  Write a map reduce job that will execute a single iteration of kKmeans 
2)  External script that will call this map reduce job many times. The script must take the output of 
the previous iteration, use it as input to map reduce.  
3)  You will have to use the distributed cache concept. Without it it is impossible to do the 
assignment correctly. 
4)  you have to provide a short accompanying document (not more than 2 pages). The document must  outline: 
    - The features selected and the reasoning 
    - Insights from clustering (needless to say that you are welcome to use Tableau, R, d3 to produce breathtaking visualizations).  
    
### Data
Recently the US Government released medicare data. It has 10  million records and it is 1 TB.
( http://www.cms.gov/ResearchKStatisticsKDataKandKSystems/StatisticsKTrendsKandKReports/MedicareKProviderKChargeKData/PhysicianKandKOtherK Supplier.html)


