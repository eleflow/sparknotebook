#!/bin/bash
wget -O /tmp/spark-1.2.0-bin-cdh4.tgz https://s3-us-west-2.amazonaws.com/sparknotebook-public/spark/spark-1.2.0-bin-cdh4.tgz 
tar -xzf /tmp/spark-1.2.0-bin-cdh4.tgz --strip-components 2 --wildcards --no-anchored 'spark-assembly*.jar' 
tar -xzf /tmp/spark-1.2.0-bin-cdh4.tgz --strip-components 2 --wildcards --no-anchored 'datanucleus*.jar' 
mkdir /opt/spark
mkdir /opt/spark/lib
cp spark-assembly*.jar /opt/spark/lib
cp datanucleus*.jar /opt/spark/lib
