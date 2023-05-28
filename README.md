# SPPU DSBDA Practical

## MapReduce Using Cloudera VM

1. Open eclipse
2. Create Project
3. Add code from github ;) according to your problem statement
4. Add external archives (right click and build path)
   - 1. hadoop-common.jar from usr/lib/hadoop
   - 2. hadoop-common-2.6.0-cdh5.4.2.jar from usr/lib/hadoop
   - 3. hadoop-core-2.6.0-mr1-cdh5.4.2.jar form usr/lib/hadoop-0.20-mapreduce
   - 4. commons-cli-1.2.jar from usr/lib/hadoop/lib
5. Create new launch configuration
6. For that click on Run as and then Run configuration
7. Click on java application and create new configuration
8. Select class name that we have created means copy pasted :D
9. After creation of launch configuration right click on project and click on export
10. Create runnable jar file
11. Give path where you want to export it and click finish
12. Open terminal and go to the path where you have exported jar file
13. Run Command to Put Data in HDFS `hadoop fs -put <file name> <path>`
14. Run command `hadoop jar <jar file name> <class name> <input file path> <output file path>`
15. Check output in output file path in HDFS `hadoop fs -cat <output file path>`

## Tableau

- Install: https://help.tableau.com/current/server-linux/en-us/jumpstart.htm
- Video1: https://www.youtube.com/watch?v=TPMlZxRRaBQ
- Video2: https://www.youtube.com/watch?v=cAHJw2o2n4c

## Hive and HBase
- Video: https://www.youtube.com/watch?v=ln8uCcz3Kgc