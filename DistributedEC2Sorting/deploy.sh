mvn clean compile assembly:single
cp target/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
FILE=publicDnsFile.txt
while read line;do
`scp -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar ubuntu@$line:~/Project`
done < $FILE
# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-52-207-220-120.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"
# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-54-173-189-130.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"
# ssh -i ~/Downloads/MyKeyPair.pem -o stricthostkeychecking=no -n -f ubuntu@ec2-54-173-215-232.compute-1.amazonaws.com "java -cp ~/Project/DistributedEC2Sorting-0.0.1-SNAPSHOT-jar-with-dependencies.jar server.Server 0 cs6240sp16 > ~/Project/log.txt &"