mvn clean compile assembly:single
cp target/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
scp -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar ubuntu@ec2-52-201-234-145.compute-1.amazonaws.com:~/Project
scp -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar ubuntu@ec2-54-152-82-241.compute-1.amazonaws.com:~/Project
# scp -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar ubuntu@ec2-54-173-215-232.compute-1.amazonaws.com:~/Project	

# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-52-207-220-120.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"
# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-54-173-189-130.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"
# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-54-173-215-232.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"