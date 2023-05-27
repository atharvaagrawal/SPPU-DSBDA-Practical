# SPPU DSBDA Practical

## MapReduce Using Cloudera VM

1. Open eclipse
2. Create folder
3. Add code from github ;) according to your problem statement
4. Add external archives (right click and build path)
   - 1. hadoop-common.jar from usr/lib/hadoop
   - 2. hadoop-mapreduce-client-core.jar form usr/lib/hadoop-mapreduce
5. Create new launch configuration
6. For that click on Run as and then Run configuration
7. Click on java application and create new configuration
8. Select class name that we have created means copy pasted :D
9. After creation of launch configuration right click on project and click on export
10. Create runnable jar file
11. Give path where you want to export it and click finish
12. Open terminal and go to the path where you have exported jar file
13. Run command `hadoop jar <jar file name> <class name> <input file path> <output file path>`
14. Check output in output file path

## Tableau

- Install: https://help.tableau.com/current/server-linux/en-us/jumpstart.htm
