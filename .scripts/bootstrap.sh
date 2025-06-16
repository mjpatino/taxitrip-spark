#!/usr/bin/env bash

sudo yum update -y

# OpenJDK 8
sudo yum install -y java-1.8.0-amazon-corretto

# Apache Hadoop
sudo wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /opt/hadoop
sudo chown -R ec2-user:ec2-user /opt/hadoop


echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc

# Apache Spark
sudo wget https://archive.apache.org/dist/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz
sudo tar -xzf spark-3.3.3-bin-hadoop3.tgz
sudo mv spark-3.3.3-bin-hadoop3 /opt/spark
sudo chown -R ec2-user:ec2-user /opt/spark

echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc

# Spark properties
sudo cp .scripts/spark-defaults.conf /opt/spark/conf/

# Spark eventLog dir
echo "mkdir -p /tmp/spark-events" >> ~/.bashrc

# Folder Initialization
sudo chmod a+x folder-initialization.sh
./folder-initialization.sh


# uname -m

# Coursier/sbt
curl -fL "https://github.com/VirtusLab/coursier-m1/releases/latest/download/cs-aarch64-pc-linux.gz" | gzip -d > cs
chmod +x cs
./cs setup

# pip3
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py

echo "Don´t forget to source ~/.bashrc"
echo "and delete installers:"
echo "  rm hadoop-3.3.6.tar.gz"
echo "  rm spark-3.3.3-bin-hadoop3.tgz"
echo "  rm cs"
echo "  rm get-pip.py"