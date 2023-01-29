## Building and Running the code
Install ant if you haven't already, on mac:
`brew install ant`

To build any of the three projects, run ant in the project directory.

To run the sample echoServer, use java -jar echoServer.jar -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>. Port numbers between 1024 and 65536 should be used (pick a random port to avoid conflicting with other services.) If you cannot start the server because “Port is already bound!” use a different port number. Avoid hard-coding port numbers for this reason.

To run the sample echoClient, use java -jar echoClient.jar

