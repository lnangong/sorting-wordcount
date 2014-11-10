Multithreaded external sorting&wordcount
========================================
Copyright (c) 2014, Long(Ryan) Nangong.  
All right reserved.

Email: lnangong@hawk.iit.edu  
Created on: Oct. 9, 2014

1. Shared memory multithreaded external sorting and word frequency count.

  a)Use Makefile to compile source code.  
  b)Ajust number of threads by modify the Parameter on top of the source code.  
  c)Execute binary code with input file argument.

2. Distributed external sorting via message passing interface(MPI).  
  
  Assume you have deployed the distributed processing module MPI on your cluster, then follow by next steps:  
  a)Compile source code with Makefile.  
  b)Configure the host file with worker node ip address.  
  c)Set number of running processes, host file, and input file argument in the run.sh scrip file.   
  d)./run.sh  to execute.
