FILE=publicDnsFile.txt
while read line;do
ssh -o stricthostkeychecking=no -i MyKeyPair.pem ubuntu@$line 'kill `pgrep java`' &
ssh -o stricthostkeychecking=no -i MyKeyPair.pem ubuntu@$line 'ps -aef | grep java' &
done < $FILE