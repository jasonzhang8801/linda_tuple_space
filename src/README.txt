Name: Sen Zhang
Email: szhang1@scu.edu

Please use the following linux command on terminal

Step 1. 
-compile all the java file
$ javac *.java

Step 2. 
-run P2.class to start the console
-P2 will create 2 directories: /tmp/<login>/linda/<hostName>/nets.txt and /tmp/<login>/linda/<hostName>/tuple.txt
-P2 will change the permission mode for both directories above
-P2 will randomly choose an available port number
-P2 will create a new thread for server to listen on given port
$ java P2 hostName

Step 3. 
-repeat Step 2 to start multiple hosts

Step 4. 
-after Step 2, linda mode start
-build the nets to connect all the available hosts
linda> add (hostName1, 123.123.12.12, 1234) (hostName2, 456.456.45.45, 4567)

Step 5.
-run the following example command to test
-for implicit type match, e.g. rd(?i:int), Client will broadcast all the hosts in the nets and spawn a new client worker thread for each hosts, including itself
linda> out(1)
linda> out("abc")
linda> rd(1)
linda> rd(?i:int)
linda> in(1)
linda> in(?i:int)
linda> rd("abc")

Step 6. 
-use ctrl + c to close P2 program
