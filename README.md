Simple Spark Word Count Java client

Objective of the project is just to demonstrate end to end application which includes 
* Sample Java code
* Maven build file 
* Script to run the driver against a Spark cluster (CDH)

For deploying this in your environment,
* Git clone the project
* Build the project using ```mvn clean package```
* Copy the SparkDemo/target/SparkDemo.jar to your CDH cluster gateway node. Typically to an application lib folder <MYAPP_LIB>
* Copy the runSparkDriver.sh to your CDH cluster gateway node. Typically to an application scripts folder
<MYAPP_SCRIPTS>

Modify the runSparkDriver.sh to have correct DRIVER_CLASSPATH location, <Spark Master URL> and <test file path> before you run the script.

**Note** This was tested using CDH5b2. Classpaths in the script have to be changed for your version of CDH.
