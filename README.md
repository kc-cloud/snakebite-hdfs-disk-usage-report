# snakebite-hdfs-disk-usage-report

Utility script to generate hdfs disk usage report using Snakebite. Snakebite is a python library that provides a pure python HDFS client and a wrapper around Hadoops minicluster. The client uses protobuf for communicating with the NameNode and comes in the form of a library and a command line interface. Currently, the snakebite client supports most actions that involve the Namenode and reading data from DataNodes. https://github.com/spotify/snakebite


## Prerequisite
 * snakebite

## usage
	
_python hdfs-disk-usage-report.py --file report.csv --size-limit 1 --levels 5_
	
* --file - name of csv file to store the report to 
* --size-limit  - drill down the folders with more than this size limit
* --level - number of levels of folders to be drilled down 
