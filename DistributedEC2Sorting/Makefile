jar:
	mvn clean compile assembly:single
	cp target/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar .

server0:
	java -cp DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16

server1:
	java -cp DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 1 cs6240sp16

server2:
	java -cp DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 2 cs6240sp16

client:
	java -cp DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar client.Client 127.0.0.1

remove:
	rm -r 0*
	rm -r 1*
	rm -r 2*
