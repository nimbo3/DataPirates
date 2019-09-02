[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=coverage)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=alert_status)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=security_rating)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=nimbo3_DataPirates&metric=vulnerabilities)](https://sonarcloud.io/dashboard?id=nimbo3_DataPirates)
# Doodle search engine

The Search engine is a thorough implementation to crawl websites. links are taken from a queue, and after checking for politeness and duplicates, their HTML docs are fetched and parsed.Finally docs are saved into databases..
This search engine is considered to be used in a way that is suitable for our usecase, but you can change it. You can setup the necessary tools according to our [wiki](https://github.com/nimbo3/DataPirates/wiki) page.

## Getting Started
First of all, DON’T PANIC. It will take 5 minutes to get the gist of what DataPirates SearchEngine is all about.

### Prerequisites
Before using the searchEngine you have setup the following tools:
* kafka
* hadoop
* hbase
* zookeeper
* elasticsearch
* redis  

A complete explanation about what version to use and how to install them is available in [wiki](https://github.com/nimbo3/DataPirates/wiki) page.

### Installing

* [Download](https://github.com/nimbo3/DataPirates/archive/master.zip) and unzip the Elasticsearch official distribution.
* Download and install maven 3+
* Create .jar file with running `mvn clean package -DskipTests` in the source directory. This will create a fat-jar in target directory of each module.
* Run jar file with `java -jar *.jar` command

## Built With

* [Maven 3.6.0](https://maven.apache.org/) - Dependency Management

## Authors

* **Alireza Asadi**
* **Hamidreza Sharifzadeh**
* **Mohammad Kazem Faghih Khorasani**
* **Mostafa Ojaghi**  

See also the list of [contributors](https://github.com/nimbo3/DataPirates/contributors) who participated in this project.

