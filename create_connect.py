import argparse
import ccloud


parser = argparse.ArgumentParser(description='Set up local connect cluster.')

parser.add_argument('--kafka-cluster', dest='kafka', metavar'lkc-xxxx', required=True, help='logical Kafka cluster to connect to')
parser.add_argument('--connect-group-id', dest='group_id', metavar='id', required=True, help='group.id of the Connect cluster')
parser.add_argument('--schema-cluster', dest='sr', metavar='lsrc-xxxx', help='schema registry resouce to connect to')

args = parser.parse_args()

topic_config = {
    'connect_status': {
        'partitions': 2,
    }
    #connect command and other topics as well
}

configs = [] #accumulator for the various configs

kafka_cluster =  args.kafka
group_id = args.group_id


# create service account for connect
cluster_sa = ccloud.create_service_account('Connect {}'.format(group_id), 'Service account for Connect cluster {}'.format(group_id))
cluster_api_key = ccloud.create_api_key(kafka_cluster, cluster_sa.id, 'Key for Connect cluster {} (Kafka)'.format(group_id))

# create topics
for name, config in topic_config.items():
    ccloud.create_topic(name, cluster=args.kafka, config=config)

    # set ACLs for topic

if args.sr:
    # configure schema-registry connection
    schema_cluster = args.sr

    #create schema registry for connect
    #create API key

for line in configs:
    print(line)
