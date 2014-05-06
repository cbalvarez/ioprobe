# ioprobe
=======

You should read http://pl.atyp.us/2013-07-perf-pitfalls.html

It just launch a few dd commands and sample every second the effective write rate.


## PARAMETERS:
-w --workers: number of child process to launch. 

-b --blocksize: block size for the write operations. Accepts k(iloByte) and M(egaByte)

-c --writecount: quantity of blocks to be written for each worker

-d --dirlist: list of directories where to write. The effective number of writers per directory is worker count / directories. It is a string representing a python list. 


## EXAMPLE:

Executes six workers, each writing 1000 block of 1Mbyte each. Three workers write in /mount/testA and other three in /mount/testB

```
 python ioprobe.py -w 6 -b 1M -c 1000 -d "['/mount/testA','/mount/testB']"
```

## TODO 
- Add reads and print reads or writes or both
- 

