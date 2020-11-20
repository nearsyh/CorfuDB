package org.corfudb.infrastructure;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
// import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealFailureRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getManagementLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureRequestMsg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CorfuManagementServerTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The ManagementServer instance used for testing
    private ManagementServer managementServer;

    // Objects that need to be mocked
    private ServerContext mockServerContext;
    private IServerRouter mockServerRouter;
    private ChannelHandlerContext mockChannelHandlerContext;

    private final AtomicInteger requestCounter = new AtomicInteger();
    private final String localEndpoint = "localhost:9000";

    /**
     * Perform the required preparation before running individual tests.
     * This includes preparing the mocks and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {


        mockServerContext = mock(ServerContext.class);
        mockServerRouter = mock(IServerRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread
        when(mockServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        when(mockServerContext.getLocalEndpoint()).thenReturn(localEndpoint);

        // managementServer = spy(new ManagementServer(mockServerContext));
    }

}