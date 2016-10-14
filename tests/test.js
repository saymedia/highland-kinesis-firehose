var assert = require('assert');
var sinon = require('sinon');

var _ = require('highland');

var toFirehose = require('../index.js');

describe('toFirehose', function () {

    it('passes data to firehose.putRecordBatch', function () {
        var s = Scenario({
            'a': 'r0',
            'b': 'r1',
            'c': 'r2',
        });
        return captureStream(s.input.pipe(
            toFirehose(s.firehose, 'stream')
        )).then(
            function (r) {
                sinon.assert.calledWith(
                    s.firehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'a'},
                            {Data: 'b'},
                            {Data: 'c'},
                        ],
                    }
                );
                assert.deepEqual(
                    r.results,
                    [
                        {
                            Record: 'a',
                            RecordId: 'r0',
                        },
                        {
                            Record: 'b',
                            RecordId: 'r1',
                        },
                        {
                            Record: 'c',
                            RecordId: 'r2',
                        },
                    ]
                );
                assert.equal(
                    r.errors.length, 0,
                    'got no errors'
                );
            }
        );
    });

    it('batches up records at the configured batch size', function () {
        var s = Scenario({
            'a': 'r0',
            'b': 'r1',
            'c': 'r2',
        });
        return captureStream(s.input.pipe(
            toFirehose(s.firehose, 'stream', {
                batchNumber: 2,
            })
        )).then(
            function (r) {
                sinon.assert.calledWith(
                    s.firehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'a'},
                            {Data: 'b'},
                        ],
                    }
                );
                sinon.assert.calledWith(
                    s.firehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'c'},
                        ],
                    }
                );
                assert.deepEqual(
                    r.results,
                    [
                        {
                            Record: 'a',
                            RecordId: 'r0',
                        },
                        {
                            Record: 'b',
                            RecordId: 'r1',
                        },
                        {
                            Record: 'c',
                            RecordId: 'r2',
                        },
                    ]
                );
                assert.equal(
                    r.errors.length, 0,
                    'got no errors'
                );
            }
        );
    });

    it('handles per-item errors', function () {
        var s = Scenario({
            'a': 'r0',
            'b': {
                ErrorCode: 'FakeError',
                ErrorMessage: 'Something very fake happened',
            },
            'c': {
                ErrorCode: 'BumpyError',
                ErrorMessage: 'Something went bump in the night',
            },
        });
        return captureStream(s.input.pipe(
            toFirehose(s.firehose, 'stream')
        )).then(
            function (r) {
                sinon.assert.calledWith(
                    s.firehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'a'},
                            {Data: 'b'},
                            {Data: 'c'},
                        ],
                    }
                );
                assert.deepEqual(
                    r.results,
                    [
                        {
                            Record: 'a',
                            RecordId: 'r0',
                        },
                    ]
                );
                assert.deepEqual(
                    r.errors,
                    [
                        {
                            Record: 'b',
                            ErrorCode: 'FakeError',
                            ErrorMessage: 'Something very fake happened',
                        },
                        {
                            Record: 'c',
                            ErrorCode: 'BumpyError',
                            ErrorMessage: 'Something went bump in the night',
                        },
                    ]
                );
            }
        );
    });

    it('handles whole-batch errors', function () {
        // We're going to submit in batches of two items, with the
        // second one failing. This requires some non-trivial
        // fakery...
        var responses = [
            {
                RequestResponses: [
                    {
                        Record: 'a',
                        RecordId: 'r0',
                    },
                    {
                        Record: 'b',
                        RecordId: 'r1',
                    },
                ]
            },
            {
                fakeError: true,
            },
            {
                RequestResponses: [
                    {
                        Record: 'e',
                        RecordId: 'r4',
                    },
                ]
            },
        ];
        var nextResponseIdx = 0;
        var fakeFirehose = {
            putRecordBatch: sinon.spy(function () {
                return {
                    promise: function () {
                        var response = responses[nextResponseIdx++];
                        if (response.fakeError) {
                            return Promise.reject({message: 'fake'});
                        }
                        else {
                            return Promise.resolve(response);
                        }
                    },
                };
            }),
        };
        var inputs = [
            'a', 'b', 'c', 'd', 'e',
        ];

        // Actual testing begins here
        return captureStream(
            _(inputs).pipe(
                toFirehose(fakeFirehose, 'stream', {
                    batchNumber: 2,
                })
            )
        ).then(
            function (r) {
                sinon.assert.calledWith(
                    fakeFirehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'a'},
                            {Data: 'b'},
                        ],
                    }
                );
                sinon.assert.calledWith(
                    fakeFirehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'c'},
                            {Data: 'd'},
                        ],
                    }
                );
                sinon.assert.calledWith(
                    fakeFirehose.putRecordBatch,
                    {
                        DeliveryStreamName: 'stream',
                        Records: [
                            {Data: 'e'},
                        ],
                    }
                );
                assert.deepEqual(
                    r.results,
                    [
                        {
                            Record: 'a',
                            RecordId: 'r0',
                        },
                        {
                            Record: 'b',
                            RecordId: 'r1',
                        },
                        {
                            Record: 'e',
                            RecordId: 'r4',
                        },
                    ]
                );
                assert.deepEqual(
                    r.errors,
                    [
                        {
                            Record: 'c',
                            ErrorCode: 'InternalFailure',
                            ErrorMessage: 'PutRecordBatch call failed: fake',
                        },
                        {
                            Record: 'd',
                            ErrorCode: 'InternalFailure',
                            ErrorMessage: 'PutRecordBatch call failed: fake',
                        },
                    ]
                );
            }
        );
    });
});

function fakeFirehose(responses) {
    var nextResponseIdx = 0;
    return {
        putRecordBatch: sinon.spy(function (req) {
            var recordCount = req.Records.length;
            return {
                promise: function () {
                    var thisBatchResponses = responses.slice(nextResponseIdx, nextResponseIdx + recordCount);
                    nextResponseIdx += recordCount;
                    return Promise.resolve({
                        RequestResponses: thisBatchResponses,
                    });
                }
            };
        }),
    };
}

function Scenario(records) {
    // Convenience utility that takes an object whose keys are
    // records to write to Firehose (which will be written as strings)
    // and whose values are the response objects that should be returned
    // for each one. Produces an object with a 'firehose' property
    // that is a fakeFirehose to produce the given result and an 'input'
    // property that is a highland stream to pipe into toFirehose that
    // produces the input.

    var input = Object.keys(records);
    var responses = input.map(function (record) {
        var resp = records[record];
        if (typeof(resp) === 'string') {
            resp = {
                RecordId: resp,
            };
        }
        return resp;
    });

    return {
        firehose: fakeFirehose(responses),
        input: _(input),
    };

}

// Utility to wrap a highland stream in a promise that returns an array
// of the results. Mocha natively supports promises, so this makes the
// tests more readable.
function captureStream(stream) {
    return new Promise(function (resolve, reject) {
        var errors = [];
        stream.errors(
            function (err) {
                errors.push(err);
            }
        ).toArray(function (results) {
            resolve({
                results: results,
                errors: errors,
            });
        });
    });
}


