import argparse
import ccloud


parser = argparse.ArgumentParser(description='Set up local connect cluster.')

parser.add_argument('--kafka-cluster', dest='kafka', metavar'lkc-xxxx', required=True, help='logical Kafka cluster to connect to')
parser.add_argument('--connect-group-id', dest='group_i', metavar='id', required=True, help='group.id of the Connect cluster')
parser.add_argument('--schema-cluster', dest='sr', metavar='lsrc-xxxx', help='schema registry resouce to connect to')

args = parser.parse_args()

topic_config = {
    'connect_status': {
        'partitions': 2,
    }
    #connect command and other topics as well
}

configs = [] #accumulator for the various configs

print('kafka: ' + args.kafka)
print('SR: ' + args.sr)

# create service account for connect
# create API key
# create topics
for name, config in topic_config.items():
    ccloud.create_topic(name, cluster=args.kafka, config=config)

    # set ACLs for topic

if args.sr:
    # configure schema-registry connection

    #create schema registry for connect
    #create API key
