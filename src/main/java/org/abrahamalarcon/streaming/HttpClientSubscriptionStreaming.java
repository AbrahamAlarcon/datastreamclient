package org.abrahamalarcon.streaming;

import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Streaming, since it is reusing the same long-lived connection to subscribe to some stream and activate the
 * subscription sending a message (STOMP send) to indicate the required data back
 * Doing this a Client can:
 * 1) establish the connection
 * 2) subscribe to some stream channel
 * 3) activate the subscription sending a message to indicate required data back
 * 4) listen on the stream channel and replies back when a response is required
 */
public class HttpClientSubscriptionStreaming
{
    private static Logger logger = Logger.getLogger(HttpClientSubscriptionStreaming.class.getName());
    private final static WebSocketHttpHeaders headers = new WebSocketHttpHeaders();
    private final static String hostname =
            //"datastream-creditmarketingapi.apps.appcanvas.net";
            "localhost";
    private final static int port =
            //80;
            8000;

    public ListenableFuture<StompSession> connect()
    {
        Transport webSocketTransport = new WebSocketTransport(new StandardWebSocketClient());
        List<Transport> transports = Collections.singletonList(webSocketTransport);

        SockJsClient sockJsClient = new SockJsClient(transports);
        sockJsClient.setMessageCodec(new Jackson2SockJsMessageCodec());

        WebSocketStompClient stompClient = new WebSocketStompClient(sockJsClient);

        String url = "ws://{host}:{port}/ws";
        return stompClient.connect(url, headers, new MyHandler(), hostname, port);
    }

    private class MyHandler extends StompSessionHandlerAdapter
    {
        public void afterConnected(StompSession stompSession, StompHeaders stompHeaders)
        {
            logger.info("Now connected");
        }
    }

    public static void main(String[] args) throws Exception
    {
        HttpClientSubscriptionStreaming client = new HttpClientSubscriptionStreaming();
        ListenableFuture<StompSession> f = client.connect();
        StompSession stompSession = f.get();

        String clientId = "client1", eventId = "geolookup";

        String stream1 = String.format("/instream/%s/%s", clientId, eventId);
        String stream2 = String.format("/outstream/%s/%s", clientId, eventId);;

        //subscribe on stream 1 and listen
        logger.info(String.format("Subscribing to stream %s", stream1) + stompSession);
        stompSession.subscribe(stream1, new StompFrameHandler()
        {
            public Type getPayloadType(StompHeaders stompHeaders)
            {
                return byte[].class;
            }
            public void handleFrame(StompHeaders stompHeaders, Object o)
            {
                logger.info(String.format("Received on %s\n", stream1) + new String((byte[]) o));
                stompSession.send(stream2, new String((byte[]) o));
            }
        });

        //send a subscription event and activate stream 1
        String endpoint = String.format("/subscription/%s/%s", clientId, eventId);
        logger.info(String.format("Sending message to %s", endpoint) + stompSession);
        //stompSession.send(endpoint, "{uuid response{version location{type tz_long wuiurl}} error{message}}".getBytes());
        stompSession.send(endpoint, "{}".getBytes());

        //subscribe on stream 2 and listen
        stompSession.subscribe(stream2, new StompFrameHandler()
        {
            public Type getPayloadType(StompHeaders stompHeaders)
            {
                return byte[].class;
            }
            public void handleFrame(StompHeaders stompHeaders, Object o)
            {
                logger.info(String.format("Received on %s\n", stream2) + new String((byte[]) o));
            }
        });

        //waits 10 minutes to receive new events on streaming channel
        Thread.sleep(100000);
    }

}
