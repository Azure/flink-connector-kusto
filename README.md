# Azure Data Explorer Connector for Apache Flink (Preview)

This library contains the source code for Azure Data Explorer DataStream Sink Connector for Apache Flink.

This sink can write data to :
[Azure Data Explorer](https://docs.microsoft.com/en-us/azure/data-explorer),
[Azure Synapse Data Explorer](https://docs.microsoft.com/en-us/azure/synapse-analytics/data-explorer/data-explorer-overview) and
[Real time analytics in Fabric](https://learn.microsoft.com/en-us/fabric/real-time-analytics/overview)

Azure Data Explorer (A.K.A. [Kusto](https://azure.microsoft.com/services/data-explorer/)) is a lightning-fast indexing and querying service.

[Flink](https://flink.apache.org/) is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams.

Making Azure Data Explorer and flink work together enables building fast and scalable applications, targeting a variety of Machine Learning, Extract-Transform-Load, Log Analytics and other data-driven scenarios.

## Changelog

For major changes from previous releases, please refer to [Releases](https://github.com/Azure/flink-connector-kusto/releases).
For known or new issues, please refer to the [issues](https://github.com/Azure/flink-connector-kusto/issues) section.

## Usage

### Linking

For Java applications using Maven project definitions, link your application with the artifact below to use the Azure Data Explorer Connector for Flink.

**In Maven**:
```
groupId = com.microsoft.azure.kusto
artifactId = flink-connector-kusto
version = 1.0.2
```

From maven central look for the following coordinates:
```
com.microsoft.azure.kusto:flink-connector-kusto:1.0.2
```
(or)
```xml
<dependency>
    <groupId>com.microsoft.azure.kusto</groupId>
    <artifactId>flink-connector-kusto</artifactId>
    <version>1.0.2</version>
</dependency>
```
Or clone this repository and build it locally to add it to your local maven repository using ```mvn clean install -DskipTests```
The jar can also be found under the [released package](https://github.com/Azure/flink-connector-kusto/releases)

#### Building Samples Module
Samples are packaged as a separate module with the following artifact

```xml
<artifactId>samples-java</artifactId>
```    

## Build Prerequisites
To use the connector, you need:

- Java 1.8 SDK installed
- [Maven 3.x](https://maven.apache.org/download.cgi) installed
- [Flink](https://flink.apache.org/)  - Tested with 1.16 and up

## Build Commands

```shell
// Clone and navigate to the repo
git clone git@github.com:Azure/flink-connector-kusto.git
cd flink-connector-kusto
// Builds jar and run all tests
mvn clean test integration-test package -DappId=<app-id> -Dcluster=<cluster> -DappKey=<app-key> -Dauthority=<tenant-d> -Ddatabase=<database>
// Builds jar, runs all tests, and installs jar to your local maven repository
mvn clean install -DappId=<app-id> -Dcluster=<cluster> -DappKey=<app-key> -Dauthority=<tenant-d> -Ddatabase=<database>
```

## Pre-Compiled Libraries
Pre compiled libraries are published to maven for usage within flink applications. 
The latest version can be found [here](https://search.maven.org/artifact/com.microsoft.azure.kusto/flink-connector-kusto)

## Dependencies
Flink Kusto connector depends on [Azure Data Explorer Data Client Library](https://mvnrepository.com/artifact/com.microsoft.azure.kusto/kusto-data)
and [Azure Data Explorer Ingest Client Library](https://mvnrepository.com/artifact/com.microsoft.azure.kusto/kusto-ingest),
available in maven repository.

## Documentation
Detailed documentation can be found [here](connector-core/docs).

## Samples
Usage examples can be found [here](samples-java)

# Need Support?

- **Have a feature request for SDKs?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time. Consider submitting a ticket for assistance from the Microsoft support team.
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/flink-connector-kusto/issues/new).

# Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).


## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
