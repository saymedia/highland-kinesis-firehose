
var _ = require('highland');

function toFirehose(firehose, streamName, opts) {
    opts = opts || {};
    var batchTime = opts.batchTime || 500;
    var batchNumber = opts.batchNumber || 200;

    return _.pipeline(
        _.batchWithTimeOrCount(batchTime, batchNumber),
        _.flatMap(function (batch) {
            var params = {
                DeliveryStreamName: streamName,
                Records: batch,
            };
            var promise = firehose.putRecordBatch(params).promise().then(
                function (data) {
                    var items = _(batch).zip(_(data.RequestResponses));
                    return items.map(function (item) {
                        var record = item[0];
                        var result = item[1];

                        if (result.ErrorCode) {
                            throw {
                                Record: record,
                                ErrorCode: result.ErrorCode,
                                ErrorMessage: result.ErrorMessage,
                            };
                        }
                        else {
                            return {
                                Record: record,
                                RecordId: result.RecordId,
                            };
                        }
                    });
                },
                function (err) {
                    // If the whole request failed then we'll emit a
                    // separate error for every item in the batch, since
                    // the caller is thinking in terms of individual
                    // records.
                    return _(batch).map(function (record) {
                        // This synthetic error mimicks what a single-item
                        // service failure might look like.
                        throw {
                            Record: record,
                            ErrorCode: 'InternalFailure',
                            ErrorMessage: 'PutRecordBatch call failed: ' + err.message,
                        };
                    });
                }
            );
            return _(promise).flatten();
        })
    );
}

module.exports = toFirehose;
