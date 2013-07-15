wonderdb
========

Highly scalable No Sql database 

Starting server:
----------------

Server can be started using provided start.bat. .sh for unix is still not present but its easy to figure out how to start on linux as well.

jar folder has wonderdbserver.jar and other dependent jar files.

If you are planning to run in standalone mode then disable zookeeper and kafka by removing following property in server.properties
zk.connect.string=localhost:2181

and disabling kafka by setting following property to false
kafka.replication.enabled=false

If you want to run in cluster mode then zookeeper and 0.8 version of kafka must be running.

command to start:
start.bat server.properties

Starting Client:
----------------

java -cp <classpath for wonderdbclient.jar and netty-3.6.1.Final.jar> <machine name> <port>

By default in server.properties, machine is localhost and port is 6060

JDBC Driver:
------------
Regiserting jdbc driver: 

DriverManager.registerDriver(new WonderDBDriver());

JDBC URL:
wonderdb://<host>:<port>

Currently, I am supporting only one host and port, but in the future I am going to support list of hosts.

server.properties:
------------------
zk.connect.string=localhost:2181
Zookeeper connection string. Just comment out this line of you want to run in standalone mode.

server.port=6060
server port

shard.port=6061
shard port: If you connect to this port, it will do CRUD operations on local database and wont send changes to the cluster. This is a good way to check
contents of local database during debugging.

kafka.replication.enabled=true
To enable kafka replication. Make sure zookeeper and kafa servers are running if this property is enabled.

kafka.broker.list=localhost:9092
kafla bokder

logFilePath=
Unused in this release

primaryCache.highWatermark=500
Primary cache high watermark value. Calculations should be about 50% of max heap size divided by 2048. So say if your max heap size if 4GB then optimal setting
for this property is: 200K

primaryCache.lowWatermark=499
Low watermark is about 10% of high watermark

primaryCache.maxSize=5000
this property value should be equal to highWatermark

secondaryCache.highWatermark=975
This setting is for secondary (DirectBuffer) memory outside of heap. Now, say your total physical memory is 48GB and heap size is 8GB
Also say you are going to run only one instance of wonderdb server on this machine and nothing else. Then assuming you keep about 12 GB for OS + 8 GB
for heap so remaining memory will be (48-8-12=18GB) now you can set value to 18GB/2K block size = 900K

secondaryCache.lowWatermark=950
Low watermark is about 10% of hist watermark

secondaryCache.maxSize=1000
same as high watermark

cacheWriter.syncTime=3000
Cache writer threads will sync to disk every 3 seconds. Leave at this value. During performace tuning this property can be tweaked.

writerThreadPool.coreSize=10
writerThreadPool.maxSize=50
writerThreadPool.queueSize=20
Above settings are for optimizing writers. For now leave it to defaults can be tweaked during performance tuning.

nettyBossThreadPool.coreSize=10
nettyBossThreadPool.maxSize=50
nettyBossThreadPool.queueSize=20
Leave it at defaults. Useful during performance tuning.

nettyWorkerThreadPool.coreSize=10
nettyWorkerThreadPool.maxSize=50
nettyWorkerThreadPool.queueSize=20
Leave it at defaults. Useful during performance tuning.
