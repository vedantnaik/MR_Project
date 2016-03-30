#!/bin/bash
instance_ids=`aws ec2 describe-instances --filters "Name=instance-type,Values=m3.medium" | jq -r ".Reservations[].Instances[].InstanceId"`
echo $instance_ids

# we get the string into an array of strings here
instance_idArray=($instance_ids)
tLen=${#instance_idArray[@]}

# we now get the length of the array, i.e., we get the number of instances that are running
echo "length of array :: "
echo $tLen

for (( i=0; i<${tLen}; i++ ));
do
	echo ${instance_idArray[i]}
	aws ec2 terminate-instances --instance-ids ${instance_idArray[i]}
	echo "in loop"
done