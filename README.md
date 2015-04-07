wonderdb
========

Highly scalable NoSql local caching (current version supports only local caching with get, put and remove apis) built using relational database concepts like:
	- block caching
	- record level locking,
	- Btree+
	- sql,
	- joins,
	- variable size blocks
	- jdbc
	
Performance testing results show:
---------------------------------------------------------------------------------------------------------------------------------------
TPS			| PAYLOAD					| MACHINE                                                                                     |
---------------------------------------------------------------------------------------------------------------------------------------
33000		| 1K bytes, 100% in memory	| amazon ec2, m3.xlarge vm (4 CPU, 13GB physical memory and 2 40GB SSDs with 3000 IOPs)		  |
---------------------------------------------------------------------------------------------------------------------------------------
12500		| 1K bytes, 80% in memory	| amazon ec2, m3.xlarge vm (4 CPU, 13GB physical memory and 2 40GB SSDs with 3000 IOPs)		  |
			|			20% in disk		|																							  |
---------------------------------------------------------------------------------------------------------------------------------------

TPS with 80/20 in memory/disk test was due to SSD bottleneck. Standard SSDs support only 3000 IOPs per second. 

Supported APIs
--------------

public byte[] get(byte[] key) - Returns data for given key.

public void put(byte[] key, byte[] value) - Adds key in BTree+ and value in disk backed concurrent list.

public void remove(byte[] key) - Removes key and value from the cache.


Settings
---------

server.properties:
------------------
primaryCache.highWatermark=500

primaryCache.lowWatermark=499

primaryCache.maxSize=5000

secondaryCache.highWatermark=975

secondaryCache.lowWatermark=950

secondaryCache.maxSize=1000

cacheWriter.syncTime=3000

cache.storage=./cache
cacheIndex.storage=./cacheIndex

disk.asyncWriterThreadPool.queueSize=10



Primary cache high watermark value. Calculations should be about 50% of max heap size divided by 2048. So say if your max heap size if 4GB then optimal setting
for this property is: 200K

Low watermark is about 10% of high watermark

this property value should be equal to highWatermark

This setting is for secondary (DirectBuffer) memory outside of heap. Now, say your total physical memory is 48GB and heap size is 8GB
Also say you are going to run only one instance of wonderdb server on this machine and nothing else. Then assuming you keep about 12 GB for OS + 8 GB
for heap so remaining memory will be (48-8-12=18GB) now you can set value to 18GB/2K block size = 900K

Low watermark is about 10% of hist watermark

same as high watermark

Cache writer threads will sync to disk every 3 seconds. Leave at this value. During performace tuning this property can be tweaked.
