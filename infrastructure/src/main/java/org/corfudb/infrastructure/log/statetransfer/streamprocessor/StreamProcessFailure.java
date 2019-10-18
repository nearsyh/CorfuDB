package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

public class StreamProcessFailure extends StateTransferException {
    public StreamProcessFailure(Throwable throwable) {
        super(throwable);
    }

    public StreamProcessFailure() {

    }
}