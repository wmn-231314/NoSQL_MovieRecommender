---
title: 'GFS Paper Reading'
date: 2021-11-21 20:56:08
tags: []
published: true
hideInList: false
feature: 
isTop: false
---
GFS
##  Abstract : A scalable distributed file system.
First, component failures are the norm rather than the
exception.Second, files are huge by traditional standards. Multi-GB
files are common.Third, most files are mutated by appending new data rather than overwriting existing data.Fourth, co-designing the applications and the file system
API benefits the overall system by increasing our flexibility

## Design Overview
### Assumptions
+ The system is built from many inexpensive commodity
components that often fail. 
+  The system stores a modest number of large files.
+ The workloads primarily consist of two kinds of reads:
large streaming reads and small random reads. In
large streaming reads, individual operations typically
read hundreds of KBs, more commonly 1 MB or more.
+ The workloads also have many large, sequential writes
that append data to files.
+ the system must efficiently implement well-defined semantics for multiple clients that concurrently append
to the same file. 
+ High sustained bandwidth is more important than low
latency
### Architecture
+ GFS cluster consists of a single master and multiple
chunkservers and is accessed by multiple clients.
It is easy to run
both a chunkserver and a client on the same machine,
+ Files are divided into fixed-size chunks. Each chunkis
identified by an immutable and globally 
+ Neither the client nor the chunkserver caches file data.
### One Master
+ we must minimize its involvement in reads and writes .irst, using the fixed chunksize, the client
translates the file name and byte offset specified by the ap-
plication into a chunkindex within the file. Then, it sends
the master a request containing the file name and chunk
index. And chunk size as 64mb  to read big files 

## system interaction
### lease and mutation order
A mutation is an operation that changes the contents or
metadata of a chunksuch as a write or an append opera-
tion.The master grants a chunklease to one of the repli-
cas, which we call the primary. The primary picks a serial
order for all mutations to the chunk. All replicas follow this order The client asks the master which chunkserver holds
the current lease for the chunkand the locations of
the other replicas. 
### Data Flow
decouple the flow of data from the flow of control .While control flows from the
client to the primary and then to all secondaries, data is
pushed linearly along a carefully picked chain of chunkservers
in a pipelined fashionFinally, we minimize latency by pipelining the data trans-
fer over TCP connections.
### GFS provides an atomic append operation called record
append.Race conditions when multiple writers do so concurrently.the primary appends the data to its replica, tells the secon-
daries to write the data at the exact offset where it has.
## MAster Operation
### Namespace 
Many master operations can take a long time: we allow multiple operations to be active and use locks over
regions of the namespace to ensure proper serialization.GFS
logically represents its namespace as a lookup table mapping
full pathnames to metadata
Chunkreplicas are created for three reasons: chunkcre-
ation, re-replication, and rebalancing.