---
title: 'BigTable Paper Reading'
date: 2021-11-21 20:55:35
tags: []
published: true
hideInList: false
feature: 
isTop: false
---
Big Table is used to solve the problem of straging the flooding data. Its demand from data latency to data size.

# Data Model

- bt is a multidimensional map is indexed by row key col key and a timestamp, each value is byte array
- row is randomly string  all atomic every row called a tablet 
- col fameilies : all data should be same type format can be like "anchor:cnnsi.com"  also can add access control. Because maybe we don't need to get all data in column family
- timestamps: in order to avoid collisions every can be stored by decresing .Make me think about the physical clock unreliable , Need use logical to replace.It can store n versions of data to implement the version control.

# Api
+ It support single row transactions with atomic.cells can be used as counter.can be used with mapreduce canbe as datasource and output end.


# Building Blocks 
using gfs to store file and log and depends on  cluster management system to scheduling. Use sstable to have a persistence kv and all are arbitrary strings.
+ each table have a sequence of blocks and the block index is stored in memory when file opend.
+ It depends on distributed block services chubby. It provides namespace with file and dir .And each one can be used as lock and all atomic.when clients 's session expires it loses all locks.They can register call back in client

# Implementation
a lib linked to every clients, one master and many tablet.master controls gc and job location  also can change schema.A Bigtable cluster stores a number of tables. Each table consists of a set of tablets, and each tablet contains all data associated with a row range
## Tablet Location
Use a b+tree of structures first includes the root tablet 's location. And metatable store the locaiton of user table . Meta alse records the 1kb and also can store log of all events



## Tablet Assignment
each tablet is assigned to a server.According to the row key, the tablet is assigned to the server that has the lowest latency to the client.When it start , it get a exclusive lock on the chubby
## Tablet Serving

persistent state is stored in gfs, updates are stored in memtable .To recover tablets ,read meta first to get the redo points.



##  Compactions

when reach a threshold , the
memtable is frozen, a new memtable is created .we bound the number of such files
by periodically executing a merging ,A merging compaction reads the contents
of a few SSTables and the memtable, and writes out a
new SSTable. A merging compaction that rewrites all SSTables
into exactly one SSTable is called a major compaction.

## Refinements
+ Locality group get many col families into group.
+ Bloom Filters  used to ask whether sstalbe is in the range of the query.
+ commit-log every tablet have two thread to write logs 
+ speeding recovery just compaction
+ Exploiting immutability the only muttable is memtable and immutable sst aloow split quickly.


# lessons
distributed system are vulnerable like memory and network corruption, large clock skew, hung machines, extended and asymmetric network partitions,overflow of GFS quotas, and planned and un- planned hardware maintenance.Solutions mayube add checksum on rpc 