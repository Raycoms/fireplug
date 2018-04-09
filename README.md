Fireplug - Byzantine fault-tolerant Architecture for Graph database

Fireplug is a flexible architecture to build robust geo-replicated graph databases. 
It can be configured to tolerate from crash to Byzantine faults, both within and across different datacenters. 
Furthermore, Fireplug is robust to bugs in existing graph database implementations, as it allows to combine multiple graph databases instances in a cohesive manner. 
Thus, Fireplug can support many different deployments, according to the performance/robustness tradeoffs imposed by the target application. 
Our evaluation shows that it can implement Byzantine fault tolerance in geo-replicated scenarios and still outperform the built-in replication mechanism of Neo4j, 
which only supports crash faults.

Currently, offering support for Neo4j, Titan, SparkSee and OrientDB





# Scripts:
All scripts require the environmental variable BAG_HOSTS which should contain a list of all ips ordered by their bft-smart ID.
All scripts are supposed to be executed outside of the project folder. (One folder up).

# Utility scripts:
- Script to kill all processes (Kills all java processes).
killjava.sh
- Script to pull the newest version and compile a new Jar.
compile.sh
- Script to setup latency between the different nodes.
addlatency.sh
- Script to generate the environmental variable and setup the config and latency files.
update_servers.py (Requires python).
-

