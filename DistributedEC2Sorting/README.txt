==================================================
README for Assignment 08: Distributed EC2 sorting
==================================================

TEAM MEMBERS: Vedant Naik, Dixit Patel, Vaibhav Tyagi, Rohan Joshi

=======================================================
Requirements and Enviroment required to run the Project
=======================================================
-	Linux environment
-	Java 1.7+
-	AWS CLI setup and working on your Linux installation
-	Ensure that the output format for the AWS CLI is set to 'JSON'
-	jq - a tool that can grep JSON data
-	A working internet connection, and the port 22 on your computer should be open
-	OpenSSH (there is no need to seperately install this, as it comes by default on almost all the Linux installations)
-	A calm mind, and lots of willpower!

======================
Contents of the folder
======================
-	Java src *** COMPLETE ***
- 	Scripts
- 	Java test

======================
How to run the project
======================
- 	The project is completely automated, just type - './start-cluster.sh 4' in the terminal
- 	This will start 4 EC2 instances. To start 8 instances, replace the 4 with 8
- 	The sort.sh script will assume that there are instances that are running and that there is a file that contains the 
	public DNS addresses of all the instances that have been started.  This script always follows the start-cluster.sh script
-	To terminate the instances, in the terminal, type './stop-cluster.sh'
-	This will destroy all the instances that are running, and will be called at the end.