package com.qh.sqlcdc.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import com.qh.sqlcdc.config.SqlCdcConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class SqlCdcSourceSplitEnumerator
        implements SourceSplitEnumerator<SqlCdcSourceSplit, ArrayList<SqlCdcSourceSplit>> {

    private final ArrayList<SqlCdcSourceSplit> pendingSplits = Lists.newArrayList();
    private final SourceSplitEnumerator.Context<SqlCdcSourceSplit> enumeratorContext;
    private SqlCdcConfig sqlCdcConfig;

    public SqlCdcSourceSplitEnumerator(
            SourceSplitEnumerator.Context<SqlCdcSourceSplit> enumeratorContext,
            SqlCdcConfig sqlCdcConfig) {
        this.enumeratorContext = enumeratorContext;
        this.sqlCdcConfig = sqlCdcConfig;
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        //        log.info("readers.toString()" + readers.toString());

    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<SqlCdcSourceSplit> splits, int subtaskId) {
        pendingSplits.addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public ArrayList<SqlCdcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
