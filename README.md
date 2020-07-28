# Aiven Kafka S3 Connector

This is a sink Kafka Connect connector that stores Kafka messages in a AWS S3 bucket.

## How to works

The connector subscribes to the specified Kafka topics and collects messages coming in them and periodically dumps the collected data to the specified bucket in AWS S3.
The connector needs the following permissions to the specified bucket:
* ``s3:GetObject``
* ``s3:PutObject``
* ``s3:AbortMultipartUpload``
* ``s3:ListMultipartUploadParts``
* ``s3:ListBucketMultipartUploads``

In case of ``Access Denied`` error see https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/

## Data Format

Connector class name, in this case: `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector`.

### S3 Object Names

S3 connector stores series of files in the specified bucket. Each object is named using pattern `[<aws.s3.prefix>]<topic>-<partition>-<startoffset>[.gz]`. The `.gz` extension is used if gzip compression is used, see `file.compression.type` below.
The connector creates one file per Kafka Connect `offset.flush.interval.ms` setting for partitions that have received new messages during that period. The setting defaults to 60 seconds.

### Data File Format

Output files are text files that contain one record per line (i.e.,
they're separated by `\n`).

The connector can output the following fields from records into the
output: the key, the value, the timestamp, and the offset. (The set and the order of
these output fields is configurable.) The field values are separated by comma.

The key and the value—if they're output—are stored as binaries encoded
in [Base64](https://en.wikipedia.org/wiki/Base64).

For example, if we output `key,value,offset,timestamp`, a record line might look like:
```
a2V5,TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQ=,1232155,1554210895
```

It is possible to control the encoding of the `value` field by setting
`format.output.fields.value.encoding` to `base64` or `none`.

If the key, the value or the timestamp is null, an empty string will be
output instead:

```
,,,1554210895
```

A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp`, `headers`, and `value`. Defaults to `value`.

## Usage

### Connector Configuration

**Important Note** Since version `2.6` all existing configuration
is deprecated and will be replaced with new one during a certain transition period (within 2-3 releases)

List of deprecated configuration parameters:
- `aws_access_key_id` - AWS Access Key ID for accessing S3 bucket. Mandatory.
- `aws_secret_access_key` - AWS S3 Secret Access Key. Mandatory.
- `aws_s3_endpoint` - The endpoint configuration (service endpoint & signing region) to be used for requests.
- `aws_s3_region` - Name of the region for the bucket used for storing the records. Defaults to `us-east-1`.
- `aws_s3_bucket` - Name of an existing bucket for storing the records. Mandatory.
- `aws_s3_prefix` - The prefix that will be added to the file name in the bucket. Can be used for putting output files into a subdirectory.
- `output_compression`- Compression type for output files. Supported algorithms are `gzip` and `none`. Defaults to `gzip`.
- `output_fields` - A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp` and `value`. Defaults to `value`.

List of new configuration parameters:
- `aws.access.key.id` - AWS Access Key ID for accessing S3 bucket. Mandatory.
- `aws.secret.access.key` - AWS S3 Secret Access Key. Mandatory.
- `aws.s3.bucket.name` - - Name of an existing bucket for storing the records. Mandatory.
- `aws.s3.endpoint` - The endpoint configuration (service endpoint & signing region) to be used for requests.
- `aws.s3.prefix` - The prefix that will be added to the file name in the bucket. Can be used for putting output files into a subdirectory.
- `aws.s3.region` - Name of the region for the bucket used for storing the records. Defaults to `us-east-1`. 
- `file.compression.type` - Compression type for output files. Supported algorithms are `gzip` and `none`. Defaults to `gzip`.
- `format.output.fields` - A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp` and `value`. Defaults to `value`.
- `format.output.fields.value.encoding` - Controls encoding of `value` field. Possible values are: `base64` and `none`. Defaults: `base64`
- `timestamp.timezone` - The time zone in which timestamps are represented. Accepts short and long standard names like: `UTC`, `PST`, `ECT`, `Europe/Berlin`, `Europe/Helsinki`, or `America/New_York`. For more information please refer to https://docs.oracle.com/javase/tutorial/datetime/iso/timezones.html. The default is `UTC`.
- `timestamp.source` -  The source of timestamps. Supports only `wallclock` which is the default value.

##### Prefix Templating
The parameter `aws_s3_prefix` or `aws.s3.prefix` supports templating using `{{ var }}` for variables that will be substituted with values.

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

Currently supported variables are:
- `topic` - the Kafka topic;
- `partition` - the Kafka partition;
- `start_offset:padding=true|false` - the Kafka offset of the first record in the file, if `padding` sets to `true` will set leading zeroes for offset, default is `false`;
- `timestamp:unit=YYYY|MM|dd|HH` - the timestamp of when the Kafka record has been processed by the connector.
   - `unit` parameter values:
     - `YYYY` - year, e.g. `2020`
     - `MM` - month, e.g. `03`
     - `dd` - day, e.g. `01`
     - `HH` - hour, e.g. `24` 

These two variables are deprecated:
- `utc_date` - the current date in UTC time zone and formatted in ISO 8601 format, e.g. `2019-03-26`
- `local_date` - the current date in the local time zone and formatted in ISO 8601 format, e.g. `2019-03-26`
please use the `timestamp` instead which described above

Examples:
- Kafka offsets with zero padding: `{{topc}}/{{partition}}/{{start_offset:padding=true}}`, generates: `some_topic/1/00000000000000000001`
- Timestamp: `{{timestamp:unit=YYYY}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/{{timestamp:unit=HH}}`, generates: `2020/03/01/00`

## Configuration

[Here](https://kafka.apache.org/documentation/#connect_running) you can
read about the Connect workers configuration and
[here](https://kafka.apache.org/documentation/#connect_resuming), about
the connector Configuration.

Here is an example connector configuration with descriptions:

```properties
### Standard connector configuration

## Fill in your values in these:

## These must have exactly these values:

# The Java class for the connector
connector.class=io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector

# The key converter for this connector
# (must be set to org.apache.kafka.connect.converters.ByteArrayConverter
# or org.apache.kafka.connect.storage.StringConverter)
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# The value converter for this connector
# (must be set to ByteArrayConverter)
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# A comma-separated list of topics to use as input for this connector
# Also a regular expression version `topics.regex` is supported.
# See https://kafka.apache.org/documentation/#connect_configuring
topics=topic1,topic2

### Connector-specific configuration
### Fill in you values
# AWS Access Key ID
aws.access.key.id=YOUR_AWS_KEY_ID

# AWS Access Secret Key
aws.secret.access.key=YOUR_AWS_SECRET_ACCESS_KEY

#AWS Region
aws.s3.region=us-east-1

#S3 prefix template
aws.s3.prefix=your-prefix/{{topic}}/

#The name of the S3 bucket to use
#Required.
aws.s3.bucket.name=my-bucket

# The set of the fields that are to be output, comma separated.
# Supported values are: `key`, `value`, `offset`, and `timestamp`.
# Optional, the default is `value`.
format.output.fields=key,value,offset,timestamp

# The compression type used for files put on GCS.
# The supported values are: `gzip`, `none`.
# Optional, the default is `none`.
file.compression.type=gzip

# The time zone in which timestamps are represented.
# Accepts short and long standard names like: `UTC`, `PST`, `ECT`,
# `Europe/Berlin`, `Europe/Helsinki`, or `America/New_York`. 
# For more information please refer to https://docs.oracle.com/javase/tutorial/datetime/iso/timezones.html.
# The default is `UTC`.
timestamp.timezone=Europe/Berlin

# The source of timestamps.
# Supports only `wallclock` which is the default value.
timestamp.source=wallclock
```

# License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
