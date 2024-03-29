#!/bin/bash
. ./script.cfg
echo "the bucket will not be created, make sure it exists"

# uncomment
programname=$1
## uncomment in final
# jarname=<<ENTER  YOUR JAR HERE>>
echo $jarname

serverjarname="CoolHadoop-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
serverarg=\"$serverjarname:$jarname\"

FILE=publicDnsFile.txt

filelength=`cat publicDnsFile.txt | wc -l`
echo $filelength
count="$((filelength-1))"
echo $count
echo $@
echo $#
# maven cleaning
# mvn clean install
# mvn clean compile assembly:single

# moving jar to current location
cp target/CoolHadoop-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
# exit
# the file that contains the public DNS addresses of the linux boxes that we just started
FILE=publicDnsFile.txt

filelength=`cat publicDnsFile.txt | wc -l` 

filelength=`expr $filelength - 1`
#echo "filelen" filelength
# moving files to server and cleaning previous run files

while read line;do
        ssh -i MyKeyPair.pem -o stricthostkeychecking=no ubuntu@$line 'kill -9 `pgrep java`' < /dev/null
        echo $line" copying"
        ssh -i MyKeyPair.pem -o stricthostkeychecking=no ubuntu@$line 'mkdir ~/Project' < /dev/null
        scp -i MyKeyPair.pem -o stricthostkeychecking=no $serverjarname ubuntu@$line:~/Project < /dev/null
        scp -i MyKeyPair.pem -o stricthostkeychecking=no $jarname ubuntu@$line:~/Project < /dev/null
done < $FILE


# This loop will ssh into the machines represented by each line generated by the starter script
# The ssh command uses stdin, and hence it directly exits the loop
# to avoid that, we attach the output to nothing, so that it can continue the loop for as long as there are
# lines in the file that we are reading from
# IMPORTANT: We will be runnning the java program on each instance, so we dont wait for one program to
# complete and exit
comds="cd ~/Project; java -cp $serverarg server.Server 0 > log.txt 2>&1"

comdsserver="cd ~/Project; java -cp $serverarg $@ > log.txt 2>&1"
echo $comds
echo $comdsserver

i=0
server="not decided"
while read line;do
        echo "$line"
        echo $i
        if [ $i -eq $count ]; then
        	server=$line
                `ssh -i MyKeyPair.pem ubuntu@$line -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "cd ~/Project; java -cp $serverarg $@ > log.txt 2>&1"` < /dev/null &
        else
        	echo "slave instance"
                `ssh -i MyKeyPair.pem ubuntu@$line -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "cd ~/Project; java -cp $serverarg server.Server $i > log.txt 2>&1"` < /dev/null &
        fi
        i=$((i+1))
done < $FILE
echo "master instance was "$server

# since we started the programs simultaneously, we will wait for the two of them to complete after this step
wait
echo "reached the end of sort script"
echo "exiting sort script"
# uncomment if you wish to download the output to your computer
echo "Please download the output from the s3 bucket to your computer"
exit