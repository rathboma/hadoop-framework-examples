organization := "com.matthewrathbone"

name := "scoobi-example"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.nicta" % "scoobi_2.10" % "0.7.1-cdh4",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.3.1",
  "junit" % "junit" % "4.8.2" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

ivyXML := (
<dependencies>
 <exclude module="jmxtools"/>
 <exclude module="jmxri"/>
 <exclude module="jms"/>
 <exclude module="hadoop-client" />
 <exclude org="org.apache" name="hadoop-core"/>
</dependencies>
)

