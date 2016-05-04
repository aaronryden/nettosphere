/*
 * Copyright 2015 Async-IO.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.atmosphere.nettosphere;

import static org.atmosphere.cpr.AtmosphereFramework.REFLECTOR_ATMOSPHEREHANDLER;
import static org.atmosphere.cpr.HeaderConfig.SSE_TRANSPORT;
import static org.atmosphere.cpr.HeaderConfig.X_ATMOSPHERE_TRANSPORT;
import static org.atmosphere.websocket.WebSocketEventListener.WebSocketEvent.TYPE.HANDSHAKE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpSession;

import org.atmosphere.container.NettyCometSupport;
import org.atmosphere.cpr.Action;
import org.atmosphere.cpr.ApplicationConfig;
import org.atmosphere.cpr.AsynchronousProcessor;
import org.atmosphere.cpr.AtmosphereConfig;
import org.atmosphere.cpr.AtmosphereFramework;
import org.atmosphere.cpr.AtmosphereHandler;
import org.atmosphere.cpr.AtmosphereInterceptor;
import org.atmosphere.cpr.AtmosphereMappingException;
import org.atmosphere.cpr.AtmosphereRequest;
import org.atmosphere.cpr.AtmosphereRequestImpl;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceImpl;
import org.atmosphere.cpr.AtmosphereResponse;
import org.atmosphere.cpr.AtmosphereResponseImpl;
import org.atmosphere.cpr.Broadcaster;
import org.atmosphere.cpr.FrameworkConfig;
import org.atmosphere.cpr.HeaderConfig;
import org.atmosphere.cpr.WebSocketProcessorFactory;
import org.atmosphere.nettosphere.util.ChannelBufferPool;
import org.atmosphere.util.FakeHttpSession;
import org.atmosphere.websocket.WebSocket;
import org.atmosphere.websocket.WebSocketEventListener;
import org.atmosphere.websocket.WebSocketHandler;
import org.atmosphere.websocket.WebSocketPingPongListener;
import org.atmosphere.websocket.WebSocketProcessor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.cookie.Cookie;
import org.jboss.netty.handler.codec.http.cookie.ServerCookieDecoder;
import org.jboss.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge the Atmosphere Framework with Netty.
 *
 * @author Jeanfrancois Arcand
 */
public class BridgeRuntime extends HttpStaticFileServerHandler {

    private final static String KEEP_ALIVE = BridgeRuntime.class.getName() + "_keep-alive";
    private static final Logger logger = LoggerFactory.getLogger(BridgeRuntime.class);
    private final AtmosphereFramework framework;
    private final Config config;
    private final ScheduledExecutorService suspendTimer;
    private final ConcurrentHashMap<String, HttpSession> sessions =
            new ConcurrentHashMap<String, HttpSession>();
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final WebSocketProcessor webSocketProcessor;
    private final ChannelGroup httpChannels = new DefaultChannelGroup("http");
    private final ChannelGroup websocketChannels = new DefaultChannelGroup("ws");
    private final ChannelBufferPool channelBufferPool;
    private final AsynchronousProcessor asynchronousProcessor;
    private final int maxWebSocketFrameSize;

    private final AtmosphereRequest proxiedRequest;
    private final AtmosphereResponse proxiedResponse;
    private final AtmosphereResource proxiedResource;

    private int webSocketTimeout = -1;

    public BridgeRuntime(final Config config) {
        super(config.path());
        this.config = config;
        this.framework = new AtmosphereFramework();

        if (config.broadcaster() != null) {
            this.framework.setDefaultBroadcasterClassName(config.broadcaster().getName());
        }

        this.framework.setAtmosphereDotXmlPath(config.configFile());

        try {
            if (config.broadcasterFactory() != null) {
                this.framework.setBroadcasterFactory(config.broadcasterFactory());
            }
        } catch (final Throwable t) {
            logger.trace("", t);
        }

        if (config.broadcasterCache() != null) {
            try {
                this.framework.setBroadcasterCacheClassName(config.broadcasterCache().getName());
            } catch (final Throwable t) {
                logger.trace("", t);
            }
        }

        final Map<String, AtmosphereHandler> handlersMap = config.handlersMap();
        for (final Map.Entry<String, AtmosphereHandler> e : handlersMap.entrySet()) {
            this.framework.addAtmosphereHandler(e.getKey(), e.getValue());
        }

        final Map<String, WebSocketHandler> webSocketHandlerMap = config.webSocketHandlersMap();

        if (handlersMap.size() == 0 && !webSocketHandlerMap.isEmpty()) {
            this.framework.addAtmosphereHandler(Broadcaster.ROOT_MASTER,
                    REFLECTOR_ATMOSPHEREHANDLER);
        }

        this.framework.getAtmosphereConfig().startupHook(new AtmosphereConfig.StartupHook() {

            @Override
            public void started(final AtmosphereFramework framework) {
                for (final Map.Entry<String, WebSocketHandler> e : webSocketHandlerMap.entrySet()) {
                    framework.addWebSocketHandler(e.getKey(), e.getValue());
                }
            }
        });

        if (config.webSocketProtocol() != null) {
            this.framework.setWebSocketProtocolClassName(config.webSocketProtocol().getName());
        }

        for (final AtmosphereInterceptor i : config.interceptors()) {
            this.framework.interceptor(i);
        }

        if (!config.scanPackages().isEmpty()) {
            for (final Class<?> s : config.scanPackages()) {
                this.framework.addAnnotationPackage(s);
            }
        }

        final Context context =
                new Context.Builder().attributes(config.servletContextAttributes())
                        .contextPath(config.mappingPath()).basePath(config.path()).build();
        final ServletContext ctx =
                (ServletContext) Proxy.newProxyInstance(BridgeRuntime.class.getClassLoader(),
                        new Class[] { ServletContext.class }, new InvocationHandler() {

                            @Override
                            public Object invoke(final Object proxy, final Method method,
                                    final Object[] args) throws Throwable {
                                try {
                                    final Method stub =
                                            Context.class.getMethod(method.getName(),
                                                    method.getParameterTypes());
                                    if (stub != null) {
                                        return stub.invoke(context, args);
                                    } else {
                                        logger.trace("Method {} not supported", method.getName());
                                        return null;
                                    }
                                } catch (final NoSuchMethodException ex) {
                                    logger.trace("Method {} not supported", method.getName());
                                    return null;
                                }
                            }
                        });

        try {
            this.framework.externalizeDestroy(true).init(
                    new NettyServletConfig(config.initParams(), ctx));
        } catch (final ServletException e) {
            throw new RuntimeException(e);
        }

        final int max =
                this.framework.getAtmosphereConfig().getInitParameter(
                        ApplicationConfig.WEBSOCKET_IDLETIME, -1);
        if (max != -1) {
            this.webSocketTimeout = max;
        }

        this.framework.setAsyncSupport(new NettyCometSupport(this.framework.getAtmosphereConfig()) {

            @Override
            public Action suspended(final AtmosphereRequest request,
                    final AtmosphereResponse response) throws IOException, ServletException {
                final Action a = super.suspended(request, response);
                if (BridgeRuntime.this.framework.getAtmosphereConfig().isSupportSession()) {
                    final AtmosphereResource r = request.resource();
                    final HttpSession s = request.getSession(true);
                    if (s != null) {
                        BridgeRuntime.this.sessions.put(r.uuid(), request.getSession(true));
                    }
                }
                return a;
            }

            @Override
            public String toString() {
                return "NettoSphereAsyncSupport";
            }
        });

        this.suspendTimer =
                new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
        this.webSocketProcessor =
                WebSocketProcessorFactory.getDefault().getWebSocketProcessor(this.framework);

        if (config.supportChunking()) {
            this.channelBufferPool =
                    new ChannelBufferPool(0, config.writeBufferPoolSize(),
                            config.writeBufferPoolCleanupFrequency(),
                            this.framework.getAtmosphereConfig());
        } else {
            // Dangerous
            this.channelBufferPool = null;
        }

        for (final String s : config.excludedInterceptors()) {
            this.framework.excludeInterceptor(s);
        }
        this.asynchronousProcessor =
                AsynchronousProcessor.class.cast(this.framework.getAsyncSupport());
        this.maxWebSocketFrameSize = config.maxWebSocketFrameSize();

        if (config.noInternalAlloc()) {
            this.proxiedRequest = new AtmosphereRequestImpl.Builder().build();
            this.proxiedResponse = new AtmosphereResponseImpl.Builder().build();
            this.proxiedResource = new AtmosphereResourceImpl();
        } else {
            this.proxiedRequest = null;
            this.proxiedResponse = null;
            this.proxiedResource = null;
        }
    }

    public AtmosphereFramework framework() {
        return this.framework;
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent messageEvent)
            throws URISyntaxException, IOException {
        final Object msg = messageEvent.getMessage();

        if (this.isShutdown.get()) {
            ctx.getChannel().close().addListener(ChannelFutureListener.CLOSE);
            return;
        }

        if (msg instanceof HttpRequest) {
            final HttpRequest r = HttpRequest.class.cast(msg);
            // Netty fail to decode headers separated by a ','
            final List<String> c = r.headers().getAll("Connection");
            final String u = r.headers().get("Upgrade");
            boolean webSocket = false;
            if (u != null && u.equalsIgnoreCase("websocket")) {
                webSocket = true;
            }

            for (final String connection : c) {
                if (connection != null && connection.toLowerCase().equalsIgnoreCase("upgrade")) {
                    webSocket = true;
                }
            }

            logger.trace("Handling request {}", r);
            if (webSocket) {
                this.handleWebSocketHandshake(ctx, messageEvent);
            } else {
                this.handleHttp(ctx, messageEvent);
            }
        } else if (msg instanceof WebSocketFrame) {
            this.handleWebSocketFrame(ctx, messageEvent);
        } else if (msg instanceof HttpChunk) {
            this.handleHttp(ctx, messageEvent);
        }
    }

    private void handleWebSocketHandshake(final ChannelHandlerContext ctx,
            final MessageEvent messageEvent) throws IOException, URISyntaxException {
        final HttpRequest request = (HttpRequest) messageEvent.getMessage();

        // Allow only GET methods.
        if (request.getMethod() != GET) {
            this.sendHttpResponse(ctx, request, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        final WebSocketServerHandshakerFactory wsFactory =
                new WebSocketServerHandshakerFactory(this.getWebSocketLocation(request),
                        this.config.subProtocols(), false, this.maxWebSocketFrameSize);

        final WebSocketServerHandshaker handshaker = wsFactory.newHandshaker(request);

        if (handshaker == null) {
            wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
        } else {

            final NettyWebSocket webSocket =
                    new NettyWebSocket(ctx.getChannel(), this.framework.getAtmosphereConfig(),
                            this.config.noInternalAlloc(), this.config.binaryWrite());
            final AtmosphereRequest atmosphereRequest = this.createAtmosphereRequest(ctx, request);

            if (!this.webSocketProcessor.handshake(atmosphereRequest)) {
                this.sendError(ctx, BAD_REQUEST, null);
                return;
            }

            this.webSocketProcessor.notifyListener(webSocket,
                    new WebSocketEventListener.WebSocketEvent("", HANDSHAKE, webSocket));

            handshaker.handshake(ctx.getChannel(), request).addListener(
                    new ChannelFutureListener() {

                        @Override
                        public void operationComplete(final ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                future.getChannel().close();
                            } else {
                                BridgeRuntime.this.websocketChannels.add(ctx.getChannel());

                                ctx.getChannel().setAttachment(webSocket);
                                if (request.headers().contains("X-wakeUpNIO")) {
                                    /**
                                     * https://github.com/AsyncHttpClient/async-
                                     * http-client/issues/471
                                     *
                                     * Netty 3.9.x has an issue and is unable to
                                     * detect the websocket frame that will be
                                     * produced if an AtmosphereInterceptor like
                                     * the JavaScriptProtocol write bytes just
                                     * after the handshake's header. The effect
                                     * is the message is lost when Netty decode
                                     * the Handshake request. This can be easily
                                     * reproduced when wAsync is used with
                                     * NettoSphere as server. For example, the
                                     * following message
                                     *
                                     * T 127.0.0.1:8080 -> 127.0.0.1:51295 [AP]
                                     * HTTP/1.1 101 Switching Protocols.
                                     * Upgrade: websocket. Connection: Upgrade.
                                     * Sec-WebSocket-Accept:
                                     * mFFTAW8KVZebToQFZZcFVWmJh8Y=. .
                                     *
                                     *
                                     * T 127.0.0.1:8080 -> 127.0.0.1:51295 [AP]
                                     * .21808c569-099b-4d8f-b657-c5965df40449|
                                     * 1391270901601
                                     *
                                     * can be lost because the Netty's Decoder
                                     * fail to realize the handshake contained a
                                     * response's body. The error doesn't occurs
                                     * all the time but under load happens more
                                     * easily.
                                     */
                                    // Wake up the remote NIO Selector so Netty
                                    // don't read the hanshake and the first
                                    // message in a single read.
                                    for (int i = 0; i < 512; i++) {
                                        webSocket.write(" ");
                                    }
                                }

                                if (BridgeRuntime.this.config.noInternalAlloc()) {
                                    webSocket.resource(BridgeRuntime.this.proxiedResource);
                                }

                                final AtmosphereResponse response =
                                        BridgeRuntime.this.config.noInternalAlloc()
                                                ? BridgeRuntime.this.proxiedResponse
                                                : AtmosphereResponseImpl.newInstance(
                                                        BridgeRuntime.this.framework
                                                                .getAtmosphereConfig(),
                                                        atmosphereRequest, webSocket);
                                BridgeRuntime.this.webSocketProcessor.open(webSocket,
                                        atmosphereRequest, response);

                                if (BridgeRuntime.this.webSocketTimeout > 0) {
                                    webSocket.closeFuture(BridgeRuntime.this.suspendTimer
                                            .scheduleAtFixedRate(
                                                    new Runnable() {

                                                        @Override
                                                        public void run() {
                                                            if (webSocket
                                                                    .lastWriteTimeStampInMilliseconds() != 0
                                                                    && (System.currentTimeMillis()
                                                                            - webSocket
                                                                                    .lastWriteTimeStampInMilliseconds() > BridgeRuntime.this.webSocketTimeout)) {
                                                                logger.debug("Timing out {}",
                                                                        webSocket);
                                                                webSocket.close();
                                                            }
                                                        }
                                                    }, BridgeRuntime.this.webSocketTimeout,
                                                    BridgeRuntime.this.webSocketTimeout,
                                                    TimeUnit.MILLISECONDS));
                                }
                            }
                        }
                    });
        }
    }

    private void handleWebSocketFrame(final ChannelHandlerContext ctx,
            final MessageEvent messageEvent) throws URISyntaxException, IOException {
        final WebSocketFrame frame = (WebSocketFrame) messageEvent.getMessage();

        logger.trace("Received frame {}", frame.getClass().getName());

        // Check for closing frame
        final Object attachment = ctx.getChannel().getAttachment();
        final WebSocket ws = (WebSocket) attachment;
        ws.setRemoteAddress(messageEvent.getRemoteAddress());
        if (frame instanceof CloseWebSocketFrame) {
            ctx.getChannel().write(frame).addListener(ChannelFutureListener.CLOSE);
        } else if (frame instanceof PingWebSocketFrame) {
            final ChannelBuffer binaryData = frame.getBinaryData();
            if (WebSocketPingPongListener.class
                    .isAssignableFrom(this.webSocketProcessor.getClass())) {
                WebSocketPingPongListener.class.cast(this.webSocketProcessor).onPing(
                        (WebSocket) attachment, binaryData.array(), binaryData.arrayOffset(),
                        binaryData.readableBytes());
            } else {
                ctx.getChannel().write(new PongWebSocketFrame(frame.getBinaryData()));
            }
        } else if (frame instanceof BinaryWebSocketFrame
                || (frame instanceof TextWebSocketFrame && this.config.textFrameAsBinary())) {
            final ChannelBuffer binaryData = frame.getBinaryData();
            this.webSocketProcessor.invokeWebSocketProtocol((WebSocket) attachment,
                    binaryData.array(), binaryData.arrayOffset(), binaryData.readableBytes());
        } else if (frame instanceof TextWebSocketFrame) {
            this.webSocketProcessor.invokeWebSocketProtocol((WebSocket) attachment,
                    ((TextWebSocketFrame) frame).getText());
        } else if (frame instanceof PongWebSocketFrame) {
            final ChannelBuffer binaryData = frame.getBinaryData();

            if (WebSocketPingPongListener.class
                    .isAssignableFrom(this.webSocketProcessor.getClass())) {
                WebSocketPingPongListener.class.cast(this.webSocketProcessor).onPong(
                        (WebSocket) attachment, binaryData.array(), binaryData.arrayOffset(),
                        binaryData.readableBytes());
            }

            if (this.config.enablePong()) {
                ctx.getChannel().write(new PingWebSocketFrame(frame.getBinaryData()));
            } else {
                logger.trace("Received Pong Frame on Channel {}", ctx.getChannel());
            }
        } else {
            logger.warn("{} frame types not supported", frame.getClass());
            ctx.getChannel().close();
        }
    }

    private AtmosphereRequest createAtmosphereRequest(final ChannelHandlerContext ctx,
            final HttpRequest request) throws URISyntaxException, UnsupportedEncodingException,
            MalformedURLException {
        if (this.config.noInternalAlloc()) {
            return this.proxiedRequest;
        }

        final String base = this.getBaseUri(request);
        final URI requestUri = new URI(base.substring(0, base.length() - 1) + request.getUri());
        final String ct = HttpHeaders.getHeader(request, "Content-Type", "text/plain");
        final long cl = HttpHeaders.getContentLength(request, 0);
        final String method = request.getMethod().getName();

        final String queryString = requestUri.getQuery();
        final Map<String, String[]> qs = new HashMap<String, String[]>();
        if (queryString != null) {
            this.parseQueryString(qs, queryString);
        }

        if (ct.equalsIgnoreCase("application/x-www-form-urlencoded")) {
            this.parseQueryString(qs, new String(request.getContent().array(), "UTF-8"));
        }

        final String u = requestUri.toURL().toString();
        final int last = u.indexOf("?") == -1 ? u.length() : u.indexOf("?");
        final String url = u.substring(0, last);
        int l;

        if (url.contains(this.config.mappingPath())) {
            l =
                    requestUri.getAuthority().length() + requestUri.getScheme().length() + 3
                            + this.config.mappingPath().length();
        } else {
            l = requestUri.getAuthority().length() + requestUri.getScheme().length() + 3;
        }

        HttpSession session = null;
        if (this.framework.getAtmosphereConfig().isSupportSession()) {
            final String[] transport = qs.get(HeaderConfig.X_ATMOSPHERE_TRANSPORT);
            if (transport != null && transport.length > 0) {
                final String[] uuid = qs.get(HeaderConfig.X_ATMOSPHERE_TRACKING_ID);
                if (uuid != null && uuid.length > 0) {
                    // TODO: Session is only supported until an unsubscribe is
                    // received.
                    if (transport[0].equalsIgnoreCase(HeaderConfig.DISCONNECT_TRANSPORT_MESSAGE)) {
                        this.sessions.remove(uuid[0]);
                    } else {
                        session = this.sessions.get(uuid[0]);

                        if (session == null) {
                            session =
                                    new FakeHttpSession("-1", null, System.currentTimeMillis(), -1);
                        }
                    }
                }
            }
        }

        final Map<String, Object> attributes = new HashMap<String, Object>();
        final AtmosphereRequestImpl.Builder requestBuilder = new AtmosphereRequestImpl.Builder();
        requestBuilder.requestURI(url.substring(l)).requestURL(url).pathInfo(url.substring(l))
        .headers(this.getHeaders(request)).method(method).contentType(ct)
        .contentLength(cl)
                // We need to read attribute after doComet
        .destroyable(false).attributes(attributes).servletPath(this.config.mappingPath())
        .session(session).cookies(this.getCookies(request)).queryStrings(qs)
        .remoteInetSocketAddress(new Callable<InetSocketAddress>() {

                    @Override
            public InetSocketAddress call() throws Exception {
                return (InetSocketAddress) ctx.getChannel().getRemoteAddress();
            }
        }).localInetSocketAddress(new Callable<InetSocketAddress>() {

            @Override
            public InetSocketAddress call() throws Exception {
                return (InetSocketAddress) ctx.getChannel().getLocalAddress();
            }
        });

        final ChannelBuffer internalBuffer = request.getContent();
        if (!this.config.aggregateRequestBodyInMemory() && !method.equalsIgnoreCase("GET")) {
            return requestBuilder.body(internalBuffer.array()).build();
        } else {
            logger.trace("Unable to read in memory the request's bytes. Using stream");
            return requestBuilder.inputStream(new ChannelBufferInputStream(internalBuffer)).build();
        }
    }

    private void handleHttp(final ChannelHandlerContext ctx, final MessageEvent messageEvent)
            throws URISyntaxException, IOException {

        boolean skipClose = false;
        AtmosphereResponse response = null;
        AtmosphereRequest request = null;
        Action a = null;
        boolean resumeOnBroadcast = false;
        boolean keptOpen = false;
        ChannelWriter asyncWriter = null;
        String method = "GET";
        final boolean writeHeader = false;
        boolean forceSuspend = false;
        final boolean aggregateBodyInMemory = this.config.aggregateRequestBodyInMemory();

        try {
            if (messageEvent.getMessage() instanceof HttpRequest) {
                final HttpRequest hrequest = (HttpRequest) messageEvent.getMessage();
                final boolean ka = HttpHeaders.isKeepAlive(hrequest);
                asyncWriter =
                        this.config.supportChunking() ? new ChunkedWriter(ctx.getChannel(), true,
                                ka, this.channelBufferPool) : new StreamWriter(ctx.getChannel(),
                                true, ka);

                        method = hrequest.getMethod().getName();

                        // First let's try to see if it's a static resources
                        if (!hrequest.getUri().contains(HeaderConfig.X_ATMOSPHERE)) {
                            try {
                                hrequest.headers().add(STATIC_MAPPING, "true");
                                super.messageReceived(ctx, messageEvent);

                                if (HttpHeaders.getHeader(hrequest, SERVICED) != null) {
                                    return;
                                }
                            } catch (final Exception e) {
                                logger.debug("Unexpected State", e);
                            } finally {
                                hrequest.headers().set(STATIC_MAPPING, "false");
                            }
                        }

                        request = this.createAtmosphereRequest(ctx, hrequest);
                        request.setAttribute(KEEP_ALIVE, new Boolean(ka));

                        // Hacky. Is the POST doesn't contains a body, we must not close
                // the connection yet.
                        final AtmosphereRequestImpl.Body b = request.body();
                        if (!aggregateBodyInMemory && !hrequest.getMethod().equals(GET) && !b.isEmpty()
                                && (b.hasString() && b.asString().isEmpty())
                                || (b.hasBytes() && b.asBytes().length == 0)) {
                            forceSuspend = true;
                        }
            } else {
                request = State.class.cast(ctx.getChannel().getAttachment()).request;
                final boolean isLast = HttpChunk.class.cast(messageEvent.getMessage()).isLast();
                final Boolean ka = (Boolean) request.getAttribute(KEEP_ALIVE);

                asyncWriter =
                        this.config.supportChunking() ? new ChunkedWriter(ctx.getChannel(), isLast,
                                ka, this.channelBufferPool) : new StreamWriter(ctx.getChannel(),
                                isLast, ka);
                        method = request.getMethod();
                        final ChannelBuffer internalBuffer =
                        HttpChunk.class.cast(messageEvent.getMessage()).getContent();

                        if (!aggregateBodyInMemory && internalBuffer.hasArray()) {
                            request.body(internalBuffer.array());
                        } else {
                            logger.trace("Unable to read in memory the request's bytes. Using stream");
                            request.body(new ChannelBufferInputStream(internalBuffer));
                        }

                        if (!isLast) {
                            forceSuspend = true;
                        }
            }

            response =
                    new AtmosphereResponseImpl.Builder().asyncIOWriter(asyncWriter)
                            .writeHeader(writeHeader).destroyable(false)
                            .header("Connection", "Keep-Alive").header("Server", "Nettosphere/2.0")
                            .request(request).build();

            if (this.config.supportChunking()) {
                response.setHeader("Transfer-Encoding", "chunked");
            }

            a = this.framework.doCometSupport(request, response);
            if (forceSuspend) {
                a.type(Action.TYPE.SUSPEND);
                // leave the stream open
                keptOpen = true;
            }

            String transport = (String) request.getAttribute(FrameworkConfig.TRANSPORT_IN_USE);
            if (transport == null) {
                transport = request.getHeader(X_ATMOSPHERE_TRANSPORT);
            }

            if (a.type() == Action.TYPE.SUSPEND) {
                if (transport != null
                        && (transport.equalsIgnoreCase(HeaderConfig.STREAMING_TRANSPORT) || transport
                                .equalsIgnoreCase(SSE_TRANSPORT))) {
                    keptOpen = true;
                } else if (transport != null
                        && (transport.equalsIgnoreCase(HeaderConfig.LONG_POLLING_TRANSPORT) || transport
                                .equalsIgnoreCase(HeaderConfig.JSONP_TRANSPORT))) {
                    resumeOnBroadcast = true;
                }
            }

            final Action action = (Action) request.getAttribute(NettyCometSupport.SUSPEND);
            final State state = new State(request, action == null ? Action.CONTINUE : action);
            ctx.getChannel().setAttachment(state);

            if (action != null && action.type() == Action.TYPE.SUSPEND) {
                if (action.timeout() != -1) {
                    final AtomicReference<ChannelWriter> w =
                            new AtomicReference<ChannelWriter>(asyncWriter);
                    final AtomicReference<Future<?>> f = new AtomicReference<Future<?>>();
                    f.set(this.suspendTimer.scheduleAtFixedRate(new Runnable() {

                        @Override
                        public void run() {
                            if (!w.get().isClosed()
                                    && (System.currentTimeMillis() - w.get().lastTick()) > action
                                            .timeout()) {
                                final AtmosphereResourceImpl impl = state.resource();
                                if (impl != null) {
                                    BridgeRuntime.this.asynchronousProcessor
                                            .endRequest(impl, false);
                                    f.get().cancel(true);
                                }
                            }
                        }
                    }, action.timeout(), action.timeout(), TimeUnit.MILLISECONDS));
                }
            } else if (action != null && action.type() == Action.TYPE.RESUME) {
                resumeOnBroadcast = false;
            }
        } catch (final AtmosphereMappingException ex) {
            if (method.equalsIgnoreCase("GET")) {
                logger.trace("Unable to map the request {}, trying static file",
                        messageEvent.getMessage());
                try {
                    skipClose = true;
                    super.messageReceived(ctx, messageEvent);
                } catch (final Exception e) {
                    logger.error("Unable to process request", e);
                    throw new IOException(e);
                }
            }
        } catch (final Throwable e) {
            logger.error("Unable to process request", e);
            throw new IOException(e);
        } finally {
            try {
                if (asyncWriter != null && !resumeOnBroadcast && !keptOpen) {
                    if (!skipClose && response != null) {
                        asyncWriter.close(response);
                    } else {
                        this.httpChannels.add(ctx.getChannel());
                    }
                }
            } finally {
                if (request != null && a != null && a.type() != Action.TYPE.SUSPEND) {
                    request.destroy();
                    response.destroy();
                    this.framework.notify(Action.TYPE.DESTROYED, request, response);
                }
            }
        }
    }

    @Override
    protected void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status,
            final MessageEvent e) {
        // For websocket, we can't send an error
        if (this.websocketChannels.contains(ctx.getChannel())) {
            logger.debug("Error {} for {}", status, e);
            ctx.getChannel().close().addListener(ChannelFutureListener.CLOSE);
        } else if (e != null) {
            final HttpRequest request = (HttpRequest) e.getMessage();
            if (HttpHeaders.getHeader(request, STATIC_MAPPING, "false").equalsIgnoreCase("false")) {
                super.sendError(ctx, status, e);
            }
        } else {
            super.sendError(ctx, status, e);
        }
    }

    public void destroy() {
        this.isShutdown.set(true);
        if (this.framework != null)
            this.framework.destroy();

        this.httpChannels.close();
        this.websocketChannels.write(new CloseWebSocketFrame());
        this.suspendTimer.shutdown();
    }

    @Override
    public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e)
            throws Exception {
        super.channelClosed(ctx, e);
        final Object o = ctx.getChannel().getAttachment();

        if (o == null)
            return;

        if (WebSocket.class.isAssignableFrom(o.getClass())) {
            final NettyWebSocket webSocket = NettyWebSocket.class.cast(o);
            logger.trace("Closing {}", webSocket.uuid());

            try {
                if (webSocket.closeFuture() != null) {
                    webSocket.closeFuture().cancel(true);
                }

                this.webSocketProcessor.close(webSocket, 1005);
            } catch (final Exception ex) {
                logger.error("{}", webSocket, ex);
            }
        } else if (State.class.isAssignableFrom(o.getClass())) {
            logger.trace("State {}", o);
            final State s = State.class.cast(o);
            if (s.action.type() == Action.TYPE.SUSPEND) {
                this.asynchronousProcessor.endRequest(s.resource(), true);
            }
        } else {
            logger.error("Invalid state {} and Channel {}", o, ctx.getChannel());
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
            throws Exception {
        // Ignore Disconnect exception.
        if (e.getCause() != null
                && (e.getCause().getClass().equals(ClosedChannelException.class) || e.getCause()
                        .getClass().equals(IOException.class))) {
            logger.trace("Exception", e.getCause());
        } else if (e.getCause() != null
                && e.getCause().getClass().equals(TooLongFrameException.class)) {
            logger.error(
                    "TooLongFrameException. The request will be closed, make sure you increase the Config.maxChunkContentLength() to a higher value.",
                    e.getCause());
            super.exceptionCaught(ctx, e);
        } else {
            logger.debug("Exception", e.getCause());
            super.exceptionCaught(ctx, e);
        }
    }

    private Map<String, String> getHeaders(final HttpRequest request) {
        final Map<String, String> headers = new HashMap<String, String>();

        for (final String name : request.headers().names()) {
            // TODO: Add support for multi header
            headers.put(name, HttpHeaders.getHeader(request, name));
        }

        return headers;
    }

    private String getBaseUri(final HttpRequest request) {
        return "http://" + HttpHeaders.getHeader(request, HttpHeaders.Names.HOST, "127.0.0.1")
                + "/";

    }

    private void parseQueryString(final Map<String, String[]> qs, final String queryString) {
        if (queryString != null) {
            final String[] s = queryString.split("&");
            for (final String a : s) {
                final String[] q = a.split("=");
                final String[] z = new String[] { q.length > 1 ? q[1] : "" };
                qs.put(q[0], z);
            }
        }
    }

    private Set<javax.servlet.http.Cookie> getCookies(final HttpRequest request) {
        final Set<javax.servlet.http.Cookie> result = new HashSet<javax.servlet.http.Cookie>();
        final String cookieHeader = request.headers().get("Cookie");
        if (cookieHeader != null) {
            final Set<Cookie> cookies = ServerCookieDecoder.LAX.decode(cookieHeader);
            for (final Cookie cookie : cookies) {
                final javax.servlet.http.Cookie c =
                        new javax.servlet.http.Cookie(cookie.name(), cookie.value());
                // Netty 3.10.2
                // if (cookie.getComment() != null) {
                // c.setComment(cookie.getComment());
                // }

                if (cookie.domain() != null) {
                    c.setDomain(cookie.domain());
                }

                c.setHttpOnly(cookie.isHttpOnly());
                c.setMaxAge(cookie.maxAge());

                if (cookie.path() != null) {
                    c.setPath(cookie.path());
                }

                c.setSecure(cookie.isSecure());
                // Netty 3.10.2
                // c.setVersion(cookie.getVersion());
                result.add(c);

            }
        }
        return result;
    }

    Config config() {
        return this.config;
    }

    private void sendHttpResponse(final ChannelHandlerContext ctx, final HttpRequest req,
            final HttpResponse res) {
        // Generate an error page if response status code is not OK (200).
        if (res.getStatus().getCode() != 200) {
            res.setContent(ChannelBuffers.copiedBuffer(res.getStatus().toString(),
                    CharsetUtil.UTF_8));
            setContentLength(res, res.getContent().readableBytes());
        }

        // Send the response and close the connection if necessary.
        final ChannelFuture f = ctx.getChannel().write(res);
        if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public final static class State {

        final AtmosphereRequest request;
        final Action action;

        public State(final AtmosphereRequest request, final Action action) {
            this.request = request;
            this.action = action;
        }

        public AtmosphereResourceImpl resource() {
            return AtmosphereResourceImpl.class.cast(this.request.resource());
        }
    }

    private String getWebSocketLocation(final HttpRequest req) {
        return "ws://" + req.headers().get(HttpHeaders.Names.HOST) + req.getUri();
    }

    private final static class NettyServletConfig implements ServletConfig {

        private final Map<String, String> initParams;
        private final ServletContext ctx;

        public NettyServletConfig(final Map<String, String> initParams, final ServletContext ctx) {
            this.initParams = initParams;
            this.ctx = ctx;
        }

        @Override
        public String getServletName() {
            return "Netty";
        }

        @Override
        public ServletContext getServletContext() {
            return this.ctx;
        }

        @Override
        public String getInitParameter(final String name) {
            return this.initParams.get(name);
        }

        @Override
        public Enumeration getInitParameterNames() {
            return Collections.enumeration(this.initParams.keySet());
        }
    }

    public ChannelGroup httpChannels() {
        return this.httpChannels;
    }

    public ChannelGroup websocketChannels() {
        return this.websocketChannels;
    }

    public ChannelBufferPool channelBufferPool() {
        return this.channelBufferPool;
    }
}
