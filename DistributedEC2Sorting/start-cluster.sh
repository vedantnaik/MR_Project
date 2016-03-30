#!/bin/bash
# Authors: Rohan Joshi
# ensure 'jq' is installed for this script to work
# uncomment for Linux (Debian based)
# sudo apt-get install jq

rm publicDnsFile.txt
rm ipAddresses.txt
nodeCount=$1
echo "we are starting "
echo $nodeCount
echo "nodes"
aws ec2 run-instances --image-id ami-663a6e0c --count $nodeCount --instance-type m3.medium --key-name MyKeyPair --security-groups my-sg
instance_ids=`aws ec2 describe-instances --filters "Name=instance-type,Values=m3.medium" | jq -r ".Reservations[].Instances[].PublicDnsName"`
instance_idArray=($instance_ids)

#this command will give 'pending' or 'running'
state=`aws ec2 describe-instances --filters "Name=dns-name,Values=${instance_idArray[0]}" | jq -r ".Reservations[].Instances[].State.Name"`

# get the length of the array
tLen=${#instance_idArray[@]}
echo "length :: "
echo $tLen
host="ubuntu@"${instance_idArray[0]}

# the list of commands that install Java on the instance and sets up the environment
# The java version that we are installing is 1.7
comds="sudo add-apt-repository -y ppa:webupd8team/java; sudo apt-get update; yes | sudo apt-get install -y oracle-java7-installer; sudo apt-get install oracle-java7-set-default; which java; mkdir ~/.aws; mkdir ~/test; mkdir ~/Project; mkdir ~/Project/sampleSortMyParts; mkdir ~/Project/credentials; ls ~/; ls ~/Project"
comds2="ls"

# for each instance that is created, we will:
# 1. Get the state of that instance.  If it is still booting, we will wait
# 2. When booting completes, we will ssh into the instance
# 3. We then install Java on the machine that we have ssh'ed into and transfer our java jar
# 4. At the end of the loop, we simply write the public DNS of each server to a text file
# 5. It is important to ensure that this file is deleted before a fresh run of the script
for (( i=0; i<${tLen}; i++ ));
do
	hostName=$host${instance_idArray[i]}
	echo $host
	state=`aws ec2 describe-instances --filters "Name=dns-name,Values=${instance_idArray[i]}" | jq -r ".Reservations[].Instances[].State.Name"`
	echo $state
	flag="False"
	echo "starting pinging loop"
	while [ $flag == "False" ]
	do
		sleep 5
		state=`aws ec2 describe-instances --filters "Name=dns-name,Values=${instance_idArray[i]}" | jq -r ".Reservations[].Instances[].State.Name"`
		echo $state
		if [ $state = 'running' ]; then
			flag="True"
		else
			flag="False"
		fi
	done
	echo "instance booted, now starting ssh"
	flagSSH="False"

	while [ $flagSSH == "False" ]
	do
		ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem ubuntu@${instance_idArray[i]} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${comds}"
		echo "ssh returned :: "
		echo $?
		temp=$?
		if [ $temp -eq 0 ]; then
			echo $temp
			echo "ssh connected successfully; success code is 0"
			flagSSH="True"
		else
			flagSSH="False"
			echo "ssh failed, trying again"
		fi
	done

	#ssh -i /Users/rohanjoshi/Documents/MyKeyPair.pem ubuntu@${instance_idArray[i]} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${comds}"
	echo "trying to log the ssh return code..."
	echo $?
	echo "transferring the jar file to the linux instance"
	scp -i /Users/rohanjoshi/Documents/MyKeyPair.pem -o stricthostkeychecking=no MyKeyPair.pem ubuntu@${instance_idArray[i]}:~/Project/credentials
	scp -i /Users/rohanjoshi/Documents/MyKeyPair.pem -o stricthostkeychecking=no Hello.java ubuntu@${instance_idArray[i]}:~/test
	echo "writing the ip to a file"
	ip=`aws ec2 describe-instances --filters "Name=dns-name,Values=${instance_idArray[i]}" | jq -r ".Reservations[].Instances[].PublicIpAddress"`
	echo $ip >> ipAddresses.txt
	echo "writing the public dns to a file"
	echo ${instance_idArray[i]} >> publicDnsFile.txt
done

echo "ssh and installation complete"
totalCountStarted="started "$nodeCount
echo "started $totalCountStarted nodes"
# call sort