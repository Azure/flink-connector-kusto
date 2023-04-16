package com.microsoft.azure.kusto;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.types.Row;

public class KustoGenericWriteAheadSink extends GenericWriteAheadSink<Row> {
    public KustoGenericWriteAheadSink(CheckpointCommitter committer,
                                      TypeSerializer<Row> serializer, String jobID)
            throws Exception {
        super(committer, serializer, jobID);
    }

    @Override
    protected boolean sendValues(Iterable<Row> iterable, long l, long l1) throws Exception {
        return false;
    }
}
