package org.corfudb.protocols.wireprotocol.orchestrator;


/**
 *
 * An interface that should be implemented by all the orchestrator service requests.
 *
 * @author Maithem
 */
public interface Request {

    /**
     * Returns the type of the request.
     *
     * @return type of request
     */
    OrchestratorRequestType getType();
}
