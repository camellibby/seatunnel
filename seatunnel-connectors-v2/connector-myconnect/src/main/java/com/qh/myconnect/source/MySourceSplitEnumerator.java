package com.qh.myconnect.source;

import com.qh.myconnect.config.MySourceOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.*;

@Slf4j
public class MySourceSplitEnumerator implements SourceSplitEnumerator<MySourceSplit, MySourceState> {

    private final SourceSplitEnumerator.Context<MySourceSplit> enumeratorContext;
    private final Map<Integer, Set<MySourceSplit>> pendingSplits;
    private final Set<MySourceSplit> assignedSplits;

    private List<MySourceOptions.DbConfig> dbConfigs;
    private final Object lock = new Object();

    public MySourceSplitEnumerator(Context<MySourceSplit> enumeratorContext, List<MySourceOptions.DbConfig> dbConfigs) {
     this(enumeratorContext,dbConfigs,null);
    }

    public MySourceSplitEnumerator(Context<MySourceSplit> enumeratorContext, List<MySourceOptions.DbConfig> dbConfigs,Set<MySourceSplit> assignedSplits) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.assignedSplits = new HashSet<>(assignedSplits);
        this.dbConfigs = dbConfigs;
    }

    @Override
    public void open() {

    }

    @Override
    public void run() throws Exception {
        discoverySplits();
        assignPendingSplits();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<MySourceSplit> splits, int subtaskId) {
        log.debug("Fake source add splits back {}, subtaskId:{}", splits, subtaskId);
        addSplitChangeToPendingAssignments(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {

    }

    @Override
    public MySourceState snapshotState(long checkpointId) throws Exception {
        log.debug("Get lock, begin snapshot fakesource split enumerator...");
        synchronized (lock) {
            log.debug("Begin snapshot fakesource split enumerator...");
            return new MySourceState(assignedSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void addSplitChangeToPendingAssignments(Collection<MySourceSplit> newSplits) {
        for (MySourceSplit split : newSplits) {
            int ownerReader = split.getSplitId() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
    }

    private void discoverySplits() {
        Set<MySourceSplit> allSplit = new HashSet<>();
        log.info("Starting to calculate splits.");
        for (int i = 0; i < dbConfigs.size(); i++) {
            int index = i;
            allSplit.add(new MySourceSplit(index, dbConfigs.get(i)));
        }
        log.info("Assigned {} to {} readers.", allSplit, dbConfigs.size());
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<MySourceSplit> pendingAssignmentForReader =
                    pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Mark pending splits as already assigned
                synchronized (lock) {
                    assignedSplits.addAll(pendingAssignmentForReader);
                    // Assign pending splits to reader
                    log.info(
                            "Assigning splits to readers {} {}",
                            pendingReader,
                            pendingAssignmentForReader);
                    enumeratorContext.assignSplit(
                            pendingReader, new ArrayList<>(pendingAssignmentForReader));
                    enumeratorContext.signalNoMoreSplits(pendingReader);
                }
            }
        }
    }
}
