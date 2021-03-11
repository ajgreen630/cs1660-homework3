import boto3
import csv
# Execution instructions:
# python3 MyAWS.py

# Secret access key information has been removed for security purposes.

# Create an instance of an s3 object:
s3 = boto3.resource('s3', aws_access_key_id = 'AKIA5P4UIVONOKYCC2HI', aws_secret_access_key = '')

# Create an S3 bucket 'aws-ajg143' (run up to the end of this section):
try:
    s3.create_bucket(Bucket = 'aws-ajg143', CreateBucketConfiguration = {'LocationConstraint' : 'us-west-2'})
except Exception as e:
    print(e)

bucket = s3.Bucket('aws-ajg143')
bucket.Acl().put(ACL = 'public-read')   # Make bucket publicly readable.

# Create the DynamoDB table.
dyndb = boto3.resource('dynamodb',
                       region_name = 'us-west-2',
                       aws_access_key_id = 'AKIA5P4UIVONOKYCC2HI',
                       aws_secret_access_key = '')
try:
    table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema = [
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")    # If table already exists..

# Wait for the table to be created...
table.meta.client.get_waiter('table_exists').wait(TableName = 'DataTable')

print(table.item_count)

# Reading the CSV file, uploading the blobs and creating the table (need to encode UTF-8 for csv.reader):
with open('/Users/alexi/Documents/Spring2021/CS1660/HW3/experiments.csv', 'rt', encoding = 'utf8') as csvfile:
    csvf = csv.reader(csvfile, delimiter = ',', quotechar = '|')
    next(csvf, None)
    for item in csvf:
        print(item)
        body = open('/Users/alexi/Documents/Spring2021/CS1660/HW3//' + item[4], 'rb')
        s3.Object('aws-ajg143', item[4]).put(Body = body)
        md = s3.Object('aws-ajg143', item[4]).Acl().put(ACL = 'public-read')

        url = "https://s3-us-west-2.amazonaws.com/aws-ajg143/" + item[4]
        metadata_item = {
                         'PartitionKey': item[0],
                         'RowKey': item[1],
                         'description': item[3],
                         'date': item[2],
                         'url': url
                        }
        try:
            table.put_item(Item = metadata_item)
        except Exception as f:
            print(f)

# Search for an item:
response = table.get_item(
    Key = {
        'PartitionKey': 'experiment1',
        'RowKey': 'data1'
    }
)

item = response['Item']
