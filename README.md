# highland-kinesis-firehose

`highland-kinesis-firehose` is highland-based library for streaming objects
into [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/).

This library can be used to easily write data into the various data sinks
that Firehose supports (Amazon S3, Redshift, and ElasticSearch at the
time of writing). This can be useful if using Highland to orchestrate an
ETL pipeline to feed data into a data warehouse, or even just as a
convenient way to write application logs into S3 where Kinesis takes care
of retries on error.

## Usage

```js
var _ = require('highland');
var toFirehose = require('highland-kinesis-firehose');
var AWS = require('aws-sdk');

// Provide data in the appropriate format for how your Firehose
// delivery stream is configured. For this example, we use CSV.
// in practice the records are more likely to be derived from a
// truly streaming source, such as event notifications.
var records = [
    new Buffer('"Shelley","Software Engineer"\n'),
    new Buffer('"Inaaya","Head of Operations"\n'),
    new Buffer('"Stephen","Office Manager"\n'),
];

_(records).pipe(
    toFirehose(new AWS.Firehose(), 'employees', {
        batchTime: 500,
        batchNumber: 100,
    })
).errors(
    function (err) {
        // The record that failed is available in err.Record
        console.error(err.ErrorCode, err.ErrorMessage);
    }
).each(
    function (result) {
        // The record that succeeded is available in result.Record
        console.log('Wrote', result.RecordId);
    }
).done(
    function () {
        console.log('All done!');
    }
);
```

The `toFirehose` function takes a pre-configured instance of the
`AWS.Firehose` client from the official AWS SDK, the name of the
Firehose delivery stream to write to, and an options object
which can contain the following options:

* `batchTime`: number of milliseconds to wait after an object is recieved
  from the stream to see if other objects show up that can be submitted
  to firehose in a single batch. The default is `500`.

* `batchNumber`: maximum number of records to include in a batch. If records
  show up so fast that this number is reached before `batchTime` elapses
  then the batch will be immediately transmitted to Firehose without waiting
  for the time to elapse. The default is `200`.

Internally the batching is using the Highland function
[`batchWithTimeOrCount`](http://highlandjs.org/#batchWithTimeOrCount); this
is handled internally for convenience so that both the input and result can
be flat streams, rather than the caller having to "un-batch" the results
and errors that are emitted.

## Installation

As usual:

```
npm install --save highland-kinesis-firehose
```

## License

Copyright 2016 Say Media, Inc.

This library may be distributed under the terms of the MIT license.
For full details, see [LICENSE](LICENSE).
