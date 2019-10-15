# Aiven Kafka S3 Connector

## Data Format

### S3 Object names

S3 connector stores series of files in the specified bucket. Each object is named using pattern `[<aws_s3_prefix>]<topic>-<partition>-<startoffset>[.gz]`. The `.gz` extension is used if gzip compression is used, see `output_compression` below.

The connector creates one file per Kafka Connect `offset.flush.interval.ms` setting for partitions that have received new messages during that period. The setting defaults to 60 seconds.

### Data file format

Data is stored in one record per line in S3. The format is comma separated fields specified by `output_fields` configuration option. If key and value fields are selected, they are written out in Base64 encoded form.

For example, `output_fields` of `value,key,timestamp` results in rows looking something like this:

```
bWVzc2FnZV9jb250ZW50,cGFydGl0aW9uX2tleQ==,1511801218777
```

## Usage

### S3 permissions

S3 connector needs the following permissions to the specified bucket:

* ``s3:GetObject``
* ``s3:PutObject``
* ``s3:AbortMultipartUpload``
* ``s3:ListMultipartUploadParts``
* ``s3:ListBucketMultipartUploads``

In case of ``Access Denied`` error see https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/

### Connector Configuration

#### `aws_access_key_id`

AWS Access Key ID for accessing S3 bucket. Mandatory.

#### `aws_secret_access_key`

AWS S3 Secret Access Key. Mandatory.

#### `aws_s3_region`

Name of the region for the bucket used for storing the records. Defaults to `us-east-1`.

#### `aws_s3_bucket`

Name of an existing bucket for storing the records. Mandatory.

#### `aws_s3_prefix`

The prefix that will be added to the file name in the bucket.
Can be used for putting output files into a subdirectory.

##### Templating

The parameter supports templating using `{{ var }}` for variables that will be substituted with values.

Currently supported variables are:
- `{{ utc_date }}` - the current date in UTC time zone.
- `{{ local_date }}` - the current date in the local time zone.

Both dates are formatted in ISO 8601 format, e.g.: `2019-03-26`.

The date of the moment when the file is being uploaded to S3 is used.

For example: `some-directory/{{ utc_date }}/`.

#### `connector.class`

Connector class name, in this case: `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector`.

#### `key.converter`

Connector-specific key encoding, must be set to `org.apache.kafka.connect.converters.ByteArrayConverter`.

#### `value.converter`

Connector-specific value encoding, must be set to `org.apache.kafka.connect.converters.ByteArrayConverter`.

#### `topics`

Topics to subscribe to. See Kafka Connect documentation for details. E.g. `demo_topic,another_topic`.

#### `output_compression`

Compression type for output files. Supported algorithms are `gzip` and `none`. Defaults to `gzip`.

#### `output_fields`

A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp` and `value`. Defaults to `value`.

### Example

S3 connector definition example:

```
curl -X POST \
    -H "Content-Type: application/json" \
    -d @- \
    https://avnadmin:password@demo-kafka.aivencloud.com:17070/connectors <<EOF
        {
            "name": "example-s3-sink",
            "config": {
                "aws_access_key_id": "AKI...",
                "aws_secret_access_key": "SECRET_ACCESS_KEY",
                "aws_s3_bucket": "aiven-example",
                "aws_s3_prefix": "example-s3-sink/",
                "aws_s3_region": "us-east-1",
                "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
                "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
                "output_compression": "gzip",
                "output_fields": "value,key,timestamp",
                "tasks.max": 1,
                "topics": "source_topic,another_topic",
                "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
            }
        }
    EOF
```
