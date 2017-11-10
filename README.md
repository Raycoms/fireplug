Fireplug - Byzantine fault-tolerant Architecture for Graph database

Fireplug is a flexible architecture to build robust geo-replicated graph databases. 
It can be configured to tolerate from crash to Byzantine faults, both within and across different datacenters. 
Furthermore, Fireplug is robust to bugs in existing graph database implementations, as it allows to combine multiple graph databases instances in a cohesive manner. 
Thus, Fireplug can support many different deployments, according to the performance/robustness tradeoffs imposed by the target application. 
Our evaluation shows that it can implement Byzantine fault tolerance in geo-replicated scenarios and still outperform the built-in replication mechanism of Neo4j, 
which only supports crash faults.

Currently, offering support for Neo4j, Titan, SparkSee and OrientDB

