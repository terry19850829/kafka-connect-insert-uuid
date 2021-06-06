

1. Review the different SMT source java files available from the default Kafka Connect transformations.
2. Write and compile your source code and unit tests. Sample unit tests for SMTs can be found in the Apache Kafka GitHub project.
3. Create your JAR file.
4. Install the JAR file. Copy your custom SMT JAR file (and any non-Kafka JAR files required by the transformation) into a directory that is under one of the directories listed in the plugin.path property in the Connect worker configuration file as shown below:
plugin.path=/usr/local/share/kafka/plugins,For example, create a directory named my-custom-smt under /usr/local/share/kafka/plugins and copy the JAR files into the my-custom-smt directory.
5. Start up the workers and the connector, and then try out your custom transformation.

ref https://docs.confluent.io/platform/current/connect/transforms/custom.html
