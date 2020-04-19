import argparse
import ccloud


parser = argparse.ArgumentParser(description='Create service account with producer rights to a topic and an API key.')

parser.add_argument('--kafka-cluster-id', dest='kafka', metavar='lkc-xxx', required=True, help='logical Kafka cluster')
parser.add_argument('--topic', dest='topic', required=True, help='Topic name')
parser.add_argument('--sa-name', dest='sa_name', required=True, help='Name of service-account')
parser.add_argument('--sa-description', dest='sa_description', help='description of service account')
parser.add_argument('--prefix', action='store_true')
parser.add_argument('--quiet', action='store_true')

args = parser.parse_args()

topic = args.topic
kafka_cluster = args.kafka
sa_name = args.sa_name
if args.sa_description:
    description = args.sa_description
else:
    description = 'SA for write access to topic '.format(topic)
is_prefix = args.prefix

kafka_sa = ccloud.create_service_account(sa_name, description)
kafka_api_key = ccloud.create_api_key(kafka_cluster, kafka_sa['id'], 'key for ' + description)
ccloud.create_acl_entry('allow', 'write', kafka_sa['id'], 'topic', topic, is_prefix)
if not args.quiet:
    print('created service account {} with rights to produce to {} using key {}'.format(sa_name, topic, kafka_api_key['key']))
    print('copy the following lines to a properties file\n')

print(ccloud.create_jaas(kafka_api_key))
