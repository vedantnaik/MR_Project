#!/bin/bash

aws ec2 run-instances --image-id ami-663a6e0c --count 1 --instance-type m3.large --key-name MyKeyPair --security-groups my-sg
instance_ids=`aws ec2 describe-instances --filters "Name=instance-type,Values=m3.large" | jq -r ".Reservations[].Instances[].PublicDnsName"`
#echo $instance_ids
instance_idArray=($instance_ids)
instance_id1=${instance_idArray[0]}
#instance_id2=${instance_idArray[1]}

echo $instance_id1
#echo $instance_id2

# get the length of the array
tLen=${#instance_idArray[@]}
echo "length :: "
echo $tLen
host="ubuntu@"${instance_idArray[0]}
echo "waiting 50 seconds"
#sleep 30s
comds="sudo add-apt-repository -y ppa:webupd8team/java; sudo apt-get update; yes | sudo apt-get install -y oracle-java7-installer; sudo apt-get install oracle-java7-set-default; which java"
comds2="ls"

for (( i=0; i<${tLen}; i++ ));
do
	hostName=$host${instance_idArray[i]}
	echo $host
	#echo ${instance_idArray[i]}
	#aws ec2 terminate-instances --instance-ids ${instance_idArray[i]}
	#ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem $hostName "${comds}"
	ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem ubuntu@${instance_idArray[i]} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${comds}"
	echo "in loop"
	sleep 20s
done
#echo "[SCRIPT MESSAGE]proceeding to ssh, please ensure that the rules allow incoming traffic from Port 22 for the protocol SSH in the security group rules"
#echo "It takes a while for the machines to start, and only then is ssh permitted"
#echo $instance_id1
#echo "starting ssh and installation for instance 1"
#ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem ubuntu@$instance_id1 "${comds}"
#echo "starting ssh and installation for instance 2"
#ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem ubuntu@$instance_id2 "${comds}"
echo "ssh and installation complete"