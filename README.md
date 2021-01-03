# kafka-bulk-create-topics
## What?
kafka-bulk-create topics is a tool to create Apache Kafka topics quickly from a CSV input file.

## Why?
kafka-bulk-create-topics is designed for using [GitOps](https://www.gitops.tech/) methodology to manage topic creation on an Apache Kafka cluster.

The expected scenario for using kafka-bulk-create-topis is:
* Users do not have ACL permissions to create topics
* You have a Git repository that contains a single file with each line defining a topic that should exist in Kafka.
* Users create pull requests adding topics they need on Kafka. These pull requests are peer-reviewed and merged by an authorised individual.
* A CI build tool will detect a change in the topics file in the git repository and invoke kafka-bulk-create-topics to create the defined topics using a system account that has the required CreateTopic permissions.

You can achieve the above with a shell script and Kafka's `kafka-topics.sh` tool. As an example:

```
$ cat /tmp/topics.txt
test1:1:1
test2:1:2
```

You can then pipe `topics.txt` in to `kafka-topics.sh`:
```
$ awk -F':' '{ system("./bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic=" $1 " --partitions=" $2 " --replication-factor=" $3) }' /tmp/topics.txt
Created topic "test1".
Created topic "test2".
```

The disadvantage of using `kafka-topics.sh` to create topics in bulk is you can only create a single topic at a time. Each time you create a topic, `kafka-topics.sh` creates a new JVM which is slow and results in significant execution time even if creating a small number of topics.
In my use-case `kafka-topics.sh` was taking 30 to 45 minutes vs seconds using `kafka-bulk-create-topics`.

`kafka-bulk-create-topics` aims to allow creating topics in bulk simple and quickly. Rust is used with the official [librdkafka](https://docs.confluent.io/2.0.0/clients/librdkafka/index.html) library. Using Rust and librdkafka means `kafka-bulk-create-topics` has a low resource, small size, statically compiled binary that is easy and quick to run and distribute.

## Usage
### configuration file
`kafka-bulk-create-topics` requires a `server.properties` style configuration file. You can find configuration properties allowed in this file at https://docs.confluent.io/5.0.0/clients/librdkafka/CONFIGURATION_8md.html.

A minimal `server.properties` must contain the `bootstrap. servers` key, i.e:

```
$ cat server.properties
bootstrap.servers=kafka00.example.com:9092
```

If your Kafka cluster has SSL or authentication enabled you will need to add the relevant configuration values defined at https://docs.confluent.io/5.0.0/clients/librdkafka/CONFIGURATION_8md.html.


### topic file
`kafka-bulk-create-topics` requires a file defining the topics to create.

The input topic files should contain a line containing a comma-separated list of values for each topic.

Each line must start with `topic_name, partition_count,replication_factor`. I.E:

```
$ cat topics.csv
my_topic,16,3
```

You can pass additional topic configurations as comma-separated key/value pairs after the replication_factor column. See https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html for a list of accepted topic configurations.

Below is an example specifying a topic `cleanup.policy` and `min.insync.replicas`.

```
$ cat topics.csv
my_topic,16,3,cleanup.policy=delete,min.insync.replicas=3
```

The topic file supports comments by prefixing them with '#'. I.E.
```
$ cat topics.csv
# This is a comment
my_topic,16,1 # This is a comment
```

To run `kafka-bulk-create-topics`:
```
$ kafka-bulk-create-topics --conf server.properties --file topics.csv
Created topic: my_topic
Total topics created: 1
```

## Limitations
`kafka-bulk-create-topics` only
* creates topics that do not exist
* does not adjust config values if a topic exists but does not match that defined in the topics csv file.

## Building
Run `cargo build --release`. This will produce the binary `target/release/kafka-bulk-create-topics`.

The output binary size is 2.8M making it small and easy to distribute.
```
$ du -h target/release/kafka-bulk-create-topics
2.8M	target/release/kafka-bulk-create-topics
```