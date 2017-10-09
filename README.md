# ECE-428-MP2

Group Member: yutao2, jiey3

##### Distributed Group Membership

Compile the code: 

```
javac DGM.java
```

Run the code:

```
java DGM
```

Then the command will show a prompt - "Your command(join, leave, showlist, showid):", so you can choose to show the ID or the membership list of the node but you have to get the node to join the group first. If you input a join command on the "introducer", the program will search the network to see if there is an existing group and join it if yes and create a new one if not.

##### False positive rate measurement:

 Compile the DGM_FP.java file and run the code like:

```
javac DGM_FP.java
java DGM_FP 3
```

Note: 3 represents for the message loss rate. You can choose to input 10 or 30 instead. If a failure is detected, it will be recorded in the log file.