## Building and Running the code
Install ant if you haven't already, on mac:
`brew install ant`

To build: run `ant build-jar` in the project root.
To run testing: run `ant test` in the project root.

To run the ECS, use java -jar m2-ecs.jar -p -a -ll

To start a server, use java -jar m2-server.jar -p -a -b -l -ll 
  
To start a client CLI, use java -jar m2-client.jar

-p <port> -a <address> -b <bootstrapServer> -l <logPath> -ll <logLevel>

Port numbers between 1024 and 65536 should be used (pick a random port to avoid conflicting with other services.) If you cannot start the server because “Port is already bound!” use a different port number. Avoid hard-coding port numbers for this reason.

An example setup:

Terminal 1: `$ java -jar m2-ecs.jar -p 5000 -a localhost`

Terminal 2: `$ java -jar m2-server.jar -p 5001 -a localhost -b localhost:5000`

Terminal 3: `$ java -jar m2-client.jar`
  
Log level will be defaulted to ALL.
  

