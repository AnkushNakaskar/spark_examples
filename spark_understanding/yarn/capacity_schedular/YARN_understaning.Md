* The first step of running a YARN application involves requesting the RM (resource manager) to create an Application Master(AM) process. 
* A client submits a job and the RM finds a Node Manager that can launch a container to host the AM process. 
* The AM process represents the client job/application. It can either run the job itself and return or request for additional resources from the RM. 
* In the latter, the RM has Node Managers on other machines launch containers on behalf of the AM process to run the distributed computation.
* Nodes chosen for new container allocations allow the computation to execute as close as possible to the input data, also known as data locality. 
* Ideally,  the container is allocated to a node hosting a replica of the data block. 
* The next preference is a node in the same rack as the input data block, and lastly any available node in the cluster. 

```
--num-executors is to request that number of executors from a cluster manager (that may also be Hadoop YARN). That's Spark's requirement.

An application master (of a YARN application) is just a thing of YARN.

It may happen that a Spark application can also be a YARN application. In such case, the Spark application gets 10 containers and one extra container for the AM.

```

```
What qualifies as a small job? By default, a small job is one that has less than 10 mappers, only one reducer, 
and an input size that is less than the size of one HDFS block. 
(Note that these values may be changed for a job by setting mapreduce.job.ubertask.maxmaps, mapreduce.job.ubertask.maxreduces, 
and mapreduce.job.ubertask.maxbytes.) 
Uber tasks must be enabled explicitly (for an individual job, or across the cluster) by setting mapreduce.job.ubertask.enable to true.

```
