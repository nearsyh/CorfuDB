package org.corfudb.logreplication.infrastructure;

import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.CustomServerRouter;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerHandshakeHandler;
import org.corfudb.infrastructure.ServerThreadFactory;

import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@Slf4j
public class CorfuReplicationServerNode implements AutoCloseable {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Map<Class, AbstractServer> serverMap;

    @Getter
    private final IServerRouter router;

    // This flag makes the closing of the CorfuServer idempotent.
    private final AtomicBoolean close;

    private ChannelFuture bindFuture;

    private boolean useNetty = true;

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     */
    public CorfuReplicationServerNode(@Nonnull ServerContext serverContext) {
        this(serverContext,
                ImmutableMap.<Class, AbstractServer>builder()
                        .put(BaseServer.class, new BaseServer(serverContext))
                        .put(LogReplicationServer.class, new LogReplicationServer(serverContext))
                        .build()
        );
    }

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     * @param serverMap     Server Map with all components.
     */
    public CorfuReplicationServerNode(@Nonnull ServerContext serverContext,
                                      @Nonnull Map<Class, AbstractServer> serverMap) {
        this.serverContext = serverContext;
        this.serverMap = serverMap;
        this.close = new AtomicBoolean(false);

        // If custom-transport is not specified by user, it will default to use Netty
        if (serverContext.getServerConfig().get("--custom-transport") != null) {
            useNetty = false;
            router = new CustomServerRouter(new ArrayList<>(serverMap.values()),
                    Integer.parseInt((String) serverContext.getServerConfig().get("<port>")));
            log.info("Corfu Replication Server initialized for CUSTOM transport.");
        } else {
            log.info("Corfu Replication Server initialized for NETTY transport.");
            router = new NettyServerRouter(new ArrayList<>(serverMap.values()));
        }

        this.serverContext.setServerRouter(router);
    }


    /**
     * Start the Corfu Replication Server by listening on the specified port.
     */
    private ChannelFuture start() {
        bindFuture = bindServer(serverContext.getBossGroup(),
                serverContext.getWorkerGroup(),
                this::configureBootstrapOptions,
                serverContext,
                (NettyServerRouter) router,
                (String) serverContext.getServerConfig().get("--address"),
                Integer.parseInt((String) serverContext.getServerConfig().get("<port>")));

        return bindFuture.syncUninterruptibly();
    }

    /**
     * Wait on Corfu Server Channel until it closes.
     */
    public void startAndListen() {
        if (useNetty) {
            // Wait on it to close.
            start().channel().closeFuture().syncUninterruptibly();
        } else {
            try {
                ((CustomServerRouter) this.router).getServerAdapter().start().get();
            } catch (Exception e) {
                throw new UnrecoverableCorfuError(e);
            }
        }
    }

    /**
     * Closes the currently running corfu server.
     */
    @Override
    public void close() {

        if (!close.compareAndSet(false, true)) {
            log.trace("close: Server already shutdown");
            return;
        }

        log.info("close: Shutting down Corfu server and cleaning resources");
        serverContext.close();

        if (useNetty) {
            bindFuture.channel().close().syncUninterruptibly();
        }

        // A executor service to create the shutdown threads
        // plus name the threads correctly.
        final ExecutorService shutdownService = Executors.newFixedThreadPool(serverMap.size(),
                new ServerThreadFactory("CorfuServer-shutdown-",
                        new ServerThreadFactory.ExceptionHandler()));

        // Turn into a list of futures on the shutdown, returning
        // generating a log message to inform of the result.
        CompletableFuture[] shutdownFutures = serverMap.values().stream()
                .map(server -> CompletableFuture.runAsync(() -> {
                    try {
                        log.info("close: Shutting down {}", server.getClass().getSimpleName());
                        server.shutdown();
                        log.info("close: Cleanly shutdown {}", server.getClass().getSimpleName());
                    } catch (Exception e) {
                        log.error("close: Failed to cleanly shutdown {}",
                                server.getClass().getSimpleName(), e);
                    }
                }, shutdownService))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(shutdownFutures).join();
        shutdownService.shutdown();
        log.info("close: Server shutdown and resources released");
    }

    /**
     * A functional interface for receiving and configuring a {@link ServerBootstrap}.
     */
    @FunctionalInterface
    public interface BootstrapConfigurer {

        /**
         * Configure a {@link ServerBootstrap}.
         *
         * @param serverBootstrap The {@link ServerBootstrap} to configure.
         */
        void configure(ServerBootstrap serverBootstrap);
    }

    /**
     * Bind the Corfu server to the given {@code port} using the provided
     * {@code channelType}. It is the callers' responsibility to shutdown the
     * {@link EventLoopGroup}s. For implementations which listen on multiple ports,
     * {@link EventLoopGroup}s may be reused.
     *
     * @param bossGroup           The "boss" {@link EventLoopGroup} which services incoming
     *                            connections.
     * @param workerGroup         The "worker" {@link EventLoopGroup} which services incoming
     *                            requests.
     * @param bootstrapConfigurer A {@link BootstrapConfigurer} which will receive the
     *                            {@link ServerBootstrap} to set options.
     * @param context             A {@link ServerContext} which will be used to configure
     *                            the server.
     * @param router              A {@link NettyServerRouter} which will process incoming
     *                            messages.
     * @param port                The port will be created on.
     * @return A {@link ChannelFuture} which can be used to wait for the server to be shutdown.
     */
    public ChannelFuture bindServer(@Nonnull EventLoopGroup bossGroup,
                                    @Nonnull EventLoopGroup workerGroup,
                                    @Nonnull BootstrapConfigurer bootstrapConfigurer,
                                    @Nonnull ServerContext context,
                                    @Nonnull NettyServerRouter router,
                                    String address,
                                    int port) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(context.getChannelImplementation().getServerChannelClass());
            bootstrapConfigurer.configure(bootstrap);

            bootstrap.childHandler(getServerChannelInitializer(context, router));
            boolean bindToAllInterfaces =
                    Optional.ofNullable(context.getServerConfig(Boolean.class, "--bind-to-all-interfaces"))
                            .orElse(false);
            if (bindToAllInterfaces) {
                log.info("Log Replication Server listening on all interfaces on port:{}", port);
                return bootstrap.bind(port).sync();
            } else {
                log.info("Log Replication Server listening on {}:{}", address, port);
                return bootstrap.bind(address, port).sync();
            }
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    /**
     * Configure server bootstrap per-channel options, such as TCP options, etc.
     *
     * @param bootstrap The {@link ServerBootstrap} to be configured.
     */
    public void configureBootstrapOptions(@Nonnull ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.SO_BACKLOG, 100)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    }


    /**
     * Obtain a {@link ChannelInitializer} which initializes the channel pipeline
     *
     * @param context The {@link ServerContext} to use.
     * @param router  The {@link NettyServerRouter} to initialize the channel with.
     * @return A {@link ChannelInitializer} to initialize the channel.
     */
    private static ChannelInitializer getServerChannelInitializer(@Nonnull ServerContext context,
                                                                  @Nonnull NettyServerRouter router) {

        // Generate the initializer.
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {
                log.info("**** Init Corfu Replication Server Node Channel");
                // Security variables
                final SslContext sslContext;
                final String[] enabledTlsProtocols;
                final String[] enabledTlsCipherSuites;

                // Security Initialization
                Boolean tlsEnabled = context.getServerConfig(Boolean.class, "--enable-tls");
                Boolean tlsMutualAuthEnabled = context.getServerConfig(Boolean.class,
                        "--enable-tls-mutual-auth");
                if (tlsEnabled) {
                    // Get the TLS cipher suites to enable
                    String ciphs = context.getServerConfig(String.class, "--tls-ciphers");
                    if (ciphs != null) {
                        enabledTlsCipherSuites = Pattern.compile(",")
                                .splitAsStream(ciphs)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsCipherSuites = new String[]{};
                    }

                    // Get the TLS protocols to enable
                    String protos = context.getServerConfig(String.class, "--tls-protocols");
                    if (protos != null) {
                        enabledTlsProtocols = Pattern.compile(",")
                                .splitAsStream(protos)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsProtocols = new String[]{};
                    }

                    try {
                        sslContext = SslContextConstructor.constructSslContext(true,
                                context.getServerConfig(String.class, "--keystore"),
                                context.getServerConfig(String.class, "--keystore-password-file"),
                                context.getServerConfig(String.class, "--truststore"),
                                context.getServerConfig(String.class,
                                        "--truststore-password-file"));
                    } catch (SSLException e) {
                        log.error("Could not build the SSL context", e);
                        throw new RuntimeException("Couldn't build the SSL context", e);
                    }
                } else {
                    enabledTlsCipherSuites = new String[]{};
                    enabledTlsProtocols = new String[]{};
                    sslContext = null;
                }

                Boolean saslPlainTextAuth = context.getServerConfig(Boolean.class,
                        "--enable-sasl-plain-text-auth");

                // If TLS is enabled, setup the encryption pipeline.
                if (tlsEnabled) {
                    SSLEngine engine = sslContext.newEngine(ch.alloc());
                    engine.setEnabledCipherSuites(enabledTlsCipherSuites);
                    engine.setEnabledProtocols(enabledTlsProtocols);
                    if (tlsMutualAuthEnabled) {
                        engine.setNeedClientAuth(true);
                    }
                    ch.pipeline().addLast("ssl", new SslHandler(engine));
                }
                // Add/parse a length field
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer
                        .MAX_VALUE, 0, 4,
                        0, 4));
                // If SASL authentication is requested, perform a SASL plain-text auth.
                if (saslPlainTextAuth) {
                    ch.pipeline().addLast("sasl/plain-text", new
                            PlainTextSaslNettyServer());
                }
                // Transform the framed message into a Corfu message.
                ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                ch.pipeline().addLast(new ServerHandshakeHandler(context.getNodeId(),
                        Version.getVersionString() + "("
                                + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")",
                        context.getServerConfig(String.class, "--HandshakeTimeout")));
                // Route the message to the server class.
                ch.pipeline().addLast(router);

                log.info("***** Finished");
            }
        };
    }

    LogReplicationServer getLogReplicationServer() {
        return (LogReplicationServer)serverMap.get(LogReplicationServer.class);
    }
}
