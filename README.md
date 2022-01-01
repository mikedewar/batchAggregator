# batchAggregator

This program is designed to chew through a bunch of parquet files containing
events, write them to a badger database grouped by a field of your choosing,
and then apply a set of aggregations to those groupings. 

Essentially it's for building features over large data sets where you don't
have access to large pieces of supporting infrastucture. 

The idea is that you have a dedicated piece of kit for building a very specific
set of features and, in those circumstances, it turns out you don't need a huge
cluster just to process a few billion events. Just a laptop and some patience. 

The design principles here are:
* read lots of small parquet files
* do as much in memory as possible
* write intermediate groupBy results to disk (I use badgerBD)
* leave sharding up to the caller
* use one binary 
* lots of useful progress bars!
* don't try to be too generic

## Protocol buffers

I'm using protocol buffers to serialise for badger.

To generate the sample .pb.go from the sample student.proto file I use

protoc student.proto --go_out=/Users/mikedewar/go/src


## Reconciliation

We pay close attention to reconciliation. Here's the process:

1. when we read an event we register it against its file
2. when we've finished reading a file we mark the file as complete, to indicate
   we dont' expect any more events from that file
3. when we've processed an event we commit it
4. at any point we can check if a file has had all its events committed


