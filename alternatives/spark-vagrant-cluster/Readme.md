# Vagrant Cluster

This is a Vagrant setup for a Spark cluster with following structure:

| IP Address | Hostname | Role |
|------------|----------|------|
|   192.168.56.101 |   master |Master|
|   192.168.56.102 |   worker1    |Slave|
|   192.168.56.103 |   worker2    |Slave|
|   192.168.56.100 |   client |Driver|


## Prerequisites
- [Vagrant](https://www.vagrantup.com/)
- [VirtualBox](https://www.virtualbox.org/)

## Usage
1. Vagrant up 
   ```bash
    cd spark-vagrant-cluster
    vagrant up
    ```
2. Start Cluster (Master, Worker1, Worker2, Client) and ensure connectivity.
    ```bash
     vagrant ssh master
     /usr/local/spark/sbin/start-master.sh
     ping worker1
     ping worker2
     ping client
     ```
3. Start Worker1 and Worker2
    ```bash
    vagrant ssh worker1
    /usr/local/spark/sbin/start-worker.sh spark://master:7077
    vagrant ssh worker2
    /usr/local/spark/sbin/start-worker.sh spark://master:7077
    ```

4. Run Spark jobs from the client node.
    ```bash
    vagrant ssh client
    cd /spark-benchmarks/
    spark-submit --master spark://master:7077 example.py
    ```

## Monitoring
- Spark Master UI: [http://master:8080](http://master:8080)