# akrivia

Instructions to Run locally form IntelliJ:

1. git clone https://github.com/arktry123/akrivia.git
2. Open the application in INtellij and Run sbt clean compile test package
3. Right-click on the PatientDataApplication.scala, and Run, please make sure you set Run configuration properly
   - This will read from the kafka topic and prcess the transformations and audits into Delta tables
4. Right-click on the DeltaTableReader class and run to see the contents of the Delta tables.
