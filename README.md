# Aiven's S3 Sink Connector for Apache Kafka

![Pull Request Workflow](https://github.com/Aiven-Open/s3-connector-for-apache-kafka/actions/workflows/main_push_and_pull_request_workflow.yml/badge.svg)

This is a sink Apache Kafka Connect connector that stores Apache Kafka messages in an AWS S3 bucket.

**Table of Contents**

- [How it works](#how-it-works)
- [Data Format](#data-format)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development](#development)


## How it works

The connector subscribes to the specified Kafka topics and collects messages coming in them and periodically dumps the collected data to the specified bucket in AWS S3.

### Requirements

The connector requires Java 11 or newer for development and production.

#### Authorization

The connector needs the following permissions to the specified bucket:
* ``s3:GetObject``
* ``s3:PutObject``
* ``s3:AbortMultipartUpload``
* ``s3:ListMultipartUploadParts``
* ``s3:ListBucketMultipartUploads``

In case of ``Access Denied`` error, see https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/

#### Authentication

To make the connector work, a user has to specify AWS credentials that allow writing to S3.
There are two ways to specify AWS credentials in this connector:

1) Long term credentials.

   It requires both `aws.access.key.id` and `aws.secret.access.key` to be specified.
2) Short term credentials.

   The connector will request a temporary token from the AWS STS service and assume a role from another AWS account.
   It requires `aws.sts.role.arn`, `aws.sts.role.session.name` to be specified.

It is important not to use both.
Using option 2, it is recommended to specify the S3 bucket region in `aws.s3.region` and the
corresponding AWS STS endpoint in `aws.sts.config.endpoint`. It's better to specify both or none.
It is also important to specify `aws.sts.role.external.id` for the security reason.
(see some details [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)).

### File name format

> File name format is tightly related to [Record Grouping](#record-grouping)

The connector uses the following format for output files (blobs):
`<prefix><filename>`.

`<prefix>`is the optional prefix that can be used, for example, for
subdirectories in the bucket. 
`<filename>` is the file name. The connector has the configurable
template for file names. 

    Configuration property `file.name.template`. If not set, default template is used: `{{topic}}-{{partition}}-{{start_offset}}`

It supports placeholders with variable names:
`{{ variable_name }}`. Currently, supported variables are:
- `topic` - the Kafka topic;
- `partition:padding=true|false` - the Kafka partition, if `padding` set to `true` it will set leading zeroes for offset, the default value is `false`;
- `start_offset:padding=true|false` - the Kafka offset of the first record in the file, if `padding` set to `true` it will set leading zeroes for offset, the default value is `false`;
- `timestamp:unit=yyyy|MM|dd|HH` - the timestamp of when the Kafka record has been processed by the connector.
   - `unit` parameter values:
     - `yyyy` - year, e.g. `2020` (please note that `YYYY` is deprecated and is interpreted as `yyyy`)
     - `MM` - month, e.g. `03`
     - `dd` - day, e.g. `01`
     - `HH` - hour, e.g. `24` 
- `key` - the Kafka key.


To add zero padding to Kafka offsets, you need to add additional parameter `padding` in the `start_offset` variable, 
which value can be `true` or `false` (the default). 
For example: `{{topic}}-{{partition}}-{{start_offset:padding=true}}.gz` 
will produce file names like `mytopic-1-00000000000000000001.gz`.

To add zero padding to partition number, you need to add additional parameter `padding` in the `partition` variable,
which value can be `true` or `false` (the default).
For example: `{{topic}}-{{partition:padding=true}}-{{start_offset}}.gz`
will produce file names like `mytopic-0000000001-1.gz`.

To add formatted timestamps, use `timestamp` variable.<br/>
For example: `{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}{{timestamp:unit=MM}}{{timestamp:unit=dd}}.gz` 
will produce file names like `mytopic-2-1-20200301.gz`.

To configure the time zone for the `timestamp` variable,
use `file.name.timestamp.timezone` property. 
Please see the description of properties in the "Configuration" section.

Only the certain combinations of variables and parameters are allowed in the file name
template (however, variables in a template can be in any order). Each
combination determines the mode of record grouping the connector will
use. Currently, supported combinations of variables and the corresponding
record grouping modes are:
- `topic`, `partition`, `start_offset`, and `timestamp` - grouping by the topic,
  partition, and timestamp;
- `key` - grouping by the key.
- `key`, `topic`, `partition` - grouping by the topic, partition, and key.

See record grouping in the next section for more details.

If the file name template is not specified, the default value is
`{{topic}}-{{partition}}-{{start_offset}}` (+ `.gz` when compression is
enabled).

### Record grouping

Incoming records are being grouped until flushed.
The connector flushes grouped records in one file per `offset.flush.interval.ms` setting for partitions that have received new messages during this period. The setting defaults to 60 seconds.

Record grouping, similar to Kafka topics, has 2 modes:

- Changelog: Connector groups all records in the order received from a Kafka topic, and stores all of them in a file.
- Compact: Connector groups all records by an identity (e.g. key) and only keeps the latest value stored in a file.

Modes are defined implicitly by the fields used of the [file name template](#file-name-format).

#### Grouping by the topic and partition

*Mode: Changelog*

In this mode, the connector groups records by the topic and partition.
When a file is written, an offset of the first record in it is added to
its name.

For example, let's say the template is
`{{topic}}-part{{partition}}-off{{start_offset}}`. If the connector
receives records like
```
topic:topicB partition:0 offset:0
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
topic:topicB partition:0 offset:1
flush
```

there will be two files `topicA-part0-off0` and `topicB-part0-off0` with
two records in each.

Each `flush` produces a new set of files. For example:

```
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
flush
topic:topicA partition:0 offset:2
topic:topicA partition:0 offset:3
flush
```

In this case, there will be two files `topicA-part0-off0` and
`topicA-part0-off2` with two records in each.

#### Grouping by the key

*Mode: Compact*

In this mode, the connector groups records by the Kafka key. It always
puts one record in a file, the latest record that arrived before a flush
for each key. Also, it overwrites files if later new records with the
same keys arrive.

This mode is good for maintaining the latest values per key as files on
GCS.

Let's say the template is `k{{key}}`. For example, when the following
records arrive
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
```

there will be two files `k0` (containing value `2`) and `k1` (containing
value `3`).

After a flush, previously written files might be overwritten:
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
key:0 value:4
flush
```

In this case, there will be two files `k0` (containing value `4`) and
`k1` (containing value `3`).

##### The string representation of a key

The connector in this mode uses the following algorithm to create the
string representation of a key:

1. If `key` is `null`, the string value is `"null"` (i.e., string
   literal `null`).
2. If `key` schema type is `STRING`, it's used directly.
3. Otherwise, Java `.toString()` is applied.

If keys of you records are strings, you may want to use
`org.apache.kafka.connect.storage.StringConverter` as `key.converter`.

##### Warning: Single key in different partitions

The `group by key` mode primarily targets scenarios where each key
appears in one partition only. If the same key appears in multiple
partitions, the result may be unexpected.

For example:
```
topic:topicA partition:0 key:x value:aaa
topic:topicA partition:1 key:x value:bbb
flush
```
file `kx` may contain `aaa` or `bbb`, i.e. the behavior is
non-deterministic.

## Data Format

Connector class name, in this case: `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector`.

### S3 Object Names

S3 connector stores series of files in the specified bucket. 
Each object is named using pattern `[<aws.s3.prefix>]<topic>-<partition>-<startoffset>[.gz]` (see [#file-name-format](File name format section) for more patterns). 
The `.gz` extension is used if gzip compression is used, see `file.compression.type` below.

### Data File Format

Output files are text files that contain one record per line (i.e.,
they're separated by `\n`) except `PARQUET` format.

There are four types of data format available: 
 - **[Default]** Flat structure, where field values are separated by comma (`csv`)

    Configuration: ```format.output.type=csv```. 
    Also, this is the default if the property is not present in the configuration.

 - Complex structure, where file is in format of [JSON lines](https://jsonlines.org/). 
    It contains one record per line and each line is a valid JSON object(`jsonl`)

    Configuration: ```format.output.type=jsonl```. 
    
 - Complex structure, where file is a valid JSON array of record objects. 
  
     Configuration: ```format.output.type=json```. 

 - Complex structure, where file is in Apache Parquet file format.

    Configuration: ```format.output.type=parquet```.

The connector can output the following fields from records into the
output: the key, the value, the timestamp, the offset and headers. (The set and the order of
output: the key, the value, the timestamp, the offset and headers. The set of
these output fields is configurable.) The field values are separated by comma.


#### CSV Format example

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

**NB!**

 - The `key.converter` property must be set to `org.apache.kafka.connect.converters.ByteArrayConverter`
or `org.apache.kafka.connect.storage.StringConverter` for this data format.

 - The `value.converter` property must be set to `org.apache.kafka.connect.converters.ByteArrayConverter` for this data format.
 
#### JSONL Format example

For example, if we output `key,value,offset,timestamp`, a record line might look like:

```json
 { "key": "k1", "value": "v0", "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
```

OR

```json
  { "key": "user1", "value": {"name": "John", "address": {"city": "London"}}, "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
```

It is recommended to use
- `org.apache.kafka.connect.storage.StringConverter` or 
- `org.apache.kafka.connect.json.JsonConverter` or
- `io.confluent.connect.avro.AvroConverter`.

as `key.converter` and/or `value.converter` to make output files human-readable.

**NB!**

 - The value of the `format.output.fields.value.encoding` property is ignored for this data format.
 - Value/Key schema will not be presented in output file, even if `value.converter.schemas.enable` property is `true`.
 But, it is still important to set this property correctly, so that connector could read records correctly. 
 
#### JSON Format example

For example, if we output `key,value,offset,timestamp`, an output file might look like:

```json
[
  { "key": "k1", "value": "v0", "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }, 
  { "key": "k2", "value": "v1", "offset": 1232156, "timestamp":"2020-01-01T00:00:05Z" }
]
```

OR

```json
[
  { "key": "user1", "value": {"name": "John", "address": {"city": "London"}}, "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
]
```

It is recommended to use
- `org.apache.kafka.connect.storage.StringConverter`, 
- `org.apache.kafka.connect.json.JsonConverter`, or
- `io.confluent.connect.avro.AvroConverter`.
 
as `key.converter` and/or `value.converter` to make output files human-readable.

**NB!**

 - The value of the `format.output.fields.value.encoding` property is ignored for this data format.
 - Value/Key schema will not be presented in output file, even if `value.converter.schemas.enable` property is `true`.
 But, it is still important to set this property correctly, so that connector could read records correctly. 


##### NB!

For both JSON and JSONL another example could be for a single field output e.g. `value`, a record line might look like:

```json
{ "value": "v0" }
```

OR

```json
{ "value": {"name": "John", "address": {"city": "London"}} }
```

In this case it sometimes make sense to get rid of additional JSON object wrapping the actual value using `format.output.envelope`.
Having `format.output.envelope=false` can produce the following output:

```json
"v0"
```

OR

```json
{"name": "John", "address": {"city": "London"}}
```

#### Parquet format example

For example, if we output `key,offset,timestamp,headers,value`, an output Parquet schema might look like this:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "RecordKeySchema"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map"},
      {"name": "value", "type": "RecordValueSchema"}
  ]
}
```
where `RecordKeySchema` - a key schema and `RecordValueSchema` - a record value schema.
This means that in case you have the record and key schema like:

Key schema:
```json
{
  "type": "string"
}
```

Record schema:
```json
{
    "type": "record", "fields": [
      {"name": "foo", "type": "string"},
      {"name": "bar", "type": "long"}
  ]
}
```
the final `Avro` schema for `Parquet` is:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "string"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map", "values": "long"},
      { "name": "value", 
        "type": "record", 
        "fields": [
          {"name": "foo", "type": "string"},
          {"name": "bar", "type": "long"}
        ]
      }
  ]
}
```


For a single-field output e.g. `value`, a record line might look like:

```json
{ "value": {"name": "John", "address": {"city": "London"}} }
```

In this case it sometimes make sense to get rid of additional JSON object wrapping the actual value using `format.output.envelope`.
Having `format.output.envelope=false` can produce the following output:

```json
{"name": "John", "address": {"city": "London"}}
```

**NB!**
- The value of the `format.output.fields.value.encoding` property is ignored for this data format.
- Due to Avro limitation message headers values must be the same datatype
- If you use `org.apache.kafka.connect.json.JsonConverter` be sure that you message contains schema. E.g. possible `JSON` message:
    ```json
    {
      "schema": {
        "type": "struct", 
        "fields": [
          {"type":"string", "field": "name"}
        ]
      }, "payload": {"name":  "foo"}
    }
    ```
- Connector works just fine with and without Schema Registry
- `format.output.envelope=false` is ignored if the value is not of type `org.apache.avro.Schema.Type.RECORD` or `org.apache.avro.Schema.Type.MAP`.

## Usage

### Connector Configuration

> **Important Note** Since version `2.6` all existing configuration
is deprecated and will be replaced with new one during a certain transition period (within 2-3 releases)

List of deprecated configuration parameters:
- `aws_access_key_id` - AWS Access Key ID for accessing S3 bucket. Mandatory.
- `aws_secret_access_key` - AWS S3 Secret Access Key. Mandatory.
- `aws_s3_endpoint` - The endpoint configuration (service endpoint & signing region) to be used for requests.
- `aws_s3_region` - Name of the region for the bucket used for storing the records. Defaults to `us-east-1`.
- `aws_s3_bucket` - Name of an existing bucket for storing the records. Mandatory.
- `aws_s3_prefix` - The prefix that will be added to the file name in the bucket. Can be used for putting output files into a subdirectory.
- `output_compression`- Compression type for output files. Supported algorithms are `gzip`, `snappy`, `zstd` and `none`. Defaults to `gzip`.
- `output_fields` - A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp` and `value`. Defaults to `value`.

List of new configuration parameters:
- `aws.access.key.id` - AWS Access Key ID for accessing S3 bucket.
- `aws.secret.access.key` - AWS S3 Secret Access Key.
- `aws.s3.bucket.name` - - Name of an existing bucket for storing the records. Mandatory. See bucket name rules: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>
- `aws.s3.endpoint` - The endpoint configuration (service endpoint & signing region) to be used for requests.
- `aws.s3.prefix` - [Deprecated] Use `file.name.prefix` and `file.name.template` instead. The prefix that will be added to the file name in the bucket. Can be used for putting output files into a subdirectory.
- `aws.s3.region` - Name of the region for the bucket used for storing the records. Defaults to `us-east-1`.
- `aws.sts.role.arn` - AWS role ARN, for cross-account access role instead of `aws.access.key.id` and `aws.secret.access.key`
- `aws.sts.role.external.id` - AWS ExternalId for cross-account access role
- `aws.sts.role.session.name` - AWS session name for cross-account access role
- `aws.sts.role.session.duration` - Session duration for cross-account access role in Seconds. Minimum value - 900. 
- `aws.sts.config.endpoint` - AWS STS endpoint for cross-account access role.
- `file.name.template` - The file name. The connector has the configurable template for file names. Constant string prefix could be added to the file name to put output files into a subdirectory.
- `file.compression.type` - Compression type for output files. Supported algorithms are `gzip`, `snappy`, `zstd` and `none`. Defaults to `gzip`.
- `format.output.fields` - A comma separated list of fields to include in output. Supported values are: `key`, `offset`, `timestamp`, `value` and `headers`. Defaults to `value`.
- `format.output.fields.value.encoding` - Controls encoding of `value` field. Possible values are: `base64` and `none`. Defaults: `base64`
- `timestamp.timezone` - The time zone in which timestamps are represented. Accepts short and long standard names like: `UTC`, `PST`, `ECT`, `Europe/Berlin`, `Europe/Helsinki`, or `America/New_York`. For more information please refer to https://docs.oracle.com/javase/tutorial/datetime/iso/timezones.html. The default is `UTC`.
- `timestamp.source` -  The source of timestamps. Supports only `wallclock` which is the default value.

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
key.converter=org.apache.kafka.connect.storage.StringConverter

# The value converter for this connector
value.converter=org.apache.kafka.connect.json.JsonConverter

# Identify, if value contains a schema.
# Required value converter is `org.apache.kafka.connect.json.JsonConverter`.
value.converter.schemas.enable=false

# The type of data format used to write data to the GCS output files.
# The supported values are: `csv`, `json`, `jsonl` and `parquet`.
# Optional, the default is `csv`.
format.output.type=jsonl

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

#File name template
file.name.template=dir1/dir2/{{topic}}-{{partition:padding=true}}-{{start_offset:padding=true}}.gz

#The name of the S3 bucket to use
#Required.
aws.s3.bucket.name=my-bucket

# The set of the fields that are to be output, comma separated.
# Supported values are: `key`, `value`, `offset`, `timestamp` and `headers`.
# Optional, the default is `value`.
format.output.fields=key,value,offset,timestamp

# The option to enable/disable wrapping of plain values into additional JSON object(aka envelope)
# Optional, the default value is `true`.
format.output.envelope=true

# The compression type used for files put on GCS.
# The supported values are: `gzip`, `snappy`, `zstd`, `none`.
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

### S3 multi-part uploads

To configure S3 multi-part uploads buffer size change:
- `aws.s3.part.size.bytes` - The Part Size in
  S3 Multi-part Uploads in bytes.
  Maximum is `2GB` and default is `5MB`.

### Retry strategy configuration

There are four configuration properties to configure retry strategy exists.

#### Apache Kafka connect retry strategy configuration property

- `kafka.retry.backoff.ms` - The retry backoff in milliseconds. This config is used to notify Apache Kafka Connect to retry delivering a message batch or
  performing recovery in case of transient exceptions. Maximum value is `24` hours.

#### AWS S3 retry strategy configuration properties

- `aws.s3.backoff.delay.ms` - S3 default base sleep time
  for non-throttled exceptions in milliseconds.
  Default is `100` ms.
- `aws.s3.backoff.max.delay.ms` - S3 maximum back-off
  time before retrying a request in milliseconds.
  Default is `20 000` ms.
- `aws.s3.backoff.max.retries` - Maximum retry limit
  (if the value is greater than 30, there can be
  integer overflow issues during delay calculation).
  Default is `3`.

## Development

### Developing together with Commons library

This project depends on [Common Module for Apache Kafka Connect](https://github.com/aiven-open/commons-for-apache-kafka-connect). Normally, an artifact of it published to a globally accessible repository is used. However, if you need to introduce changes to both this connector and Commons library at the same time, you should short-circuit the development loop via locally published artifacts. Please follow this steps:
1. Checkout the `main` `HEAD` of Commons.
2. Ensure the version [here](https://github.com/aiven-open/commons-for-apache-kafka-connect/blob/main/gradle.properties) is with `-SNAPSHOT` prefix.
3. Make changes to Commons.
4. Publish it locally with `./gradlew publishToMavenLocal`.
5. Change the version in the connector's [`build.gradle`](build.gradle) (`ext.aivenConnectCommonsVersion`) to match the published snapshot version of Commons.

After that, the latest changes you've done to Commons will be used.

When you finish developing the feature and is sure Commons won't need to change:
1. Make a proper release of Commons.
2. Publish the artifact to the currently used globally accessible repository.
3. Change the version of Commons in the connector to the published one.

### Integration testing

Integration tests are implemented using JUnit, Gradle and Docker.

To run them, you need:
- Docker installed.

Integration testing doesn't require valid AWS credentials.

To simulate AWS S3 behaviour, tests use [LocalStack](https://github.com/localstack/localstack-java-utils).

In order to run the integration tests, execute from the project root
directory:

```bash
./gradlew clean integrationTest
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Trademarks

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. AWS S3 is a trademark and property of their respective owners. All product and service names used in this website are for identification purposes only and do not imply endorsement.
