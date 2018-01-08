package org.abrahamalarcon;

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

public class HttpClient
{
    private static Logger logger = Logger.getLogger(HttpClient.class.getName());
    private final static WebSocketHttpHeaders headers = new WebSocketHttpHeaders();

    public ListenableFuture<StompSession> connect()
    {
        Transport webSocketTransport = new WebSocketTransport(new StandardWebSocketClient());
        List<Transport> transports = Collections.singletonList(webSocketTransport);

        SockJsClient sockJsClient = new SockJsClient(transports);
        sockJsClient.setMessageCodec(new Jackson2SockJsMessageCodec());

        WebSocketStompClient stompClient = new WebSocketStompClient(sockJsClient);

        String url = "ws://{host}:{port}/ws";
        return stompClient.connect(url, headers, new MyHandler(), "localhost", 8000);
    }

    public void subscribe(String topic, StompSession stompSession) throws ExecutionException, InterruptedException
    {
        stompSession.subscribe(topic, new StompFrameHandler()
        {
            public Type getPayloadType(StompHeaders stompHeaders)
            {
                return byte[].class;
            }

            public void handleFrame(StompHeaders stompHeaders, Object o)
            {
                logger.info("Received " + new String((byte[]) o));
            }
        });
    }

    public void send(String endpoint, String message, StompSession stompSession)
    {
        stompSession.send(endpoint, message.getBytes());
    }

    private class MyHandler extends StompSessionHandlerAdapter
    {
        public void afterConnected(StompSession stompSession, StompHeaders stompHeaders)
        {
            logger.info("Now connected");
        }
    }

    public static void main(String[] args) throws Exception {
        HttpClient client = new HttpClient();

        ListenableFuture<StompSession> f = client.connect();
        StompSession stompSession = f.get();

        String clientId = "client1", eventId = "geolookup";

        String topic = String.format("/queue/%s/%s", clientId, eventId);
        logger.info(String.format("Subscribing to topic %s using session ", topic) + stompSession);
        client.subscribe(topic, stompSession);

        for(int i = 1; i < 6; i++)
        {
            String endpoint = String.format("/stream/notifyme/%s/%s/%s", clientId, eventId, i);
            logger.info(String.format("Sending message to %s", endpoint) + stompSession);
            client.send(endpoint, "{\"country\":\"Chile\",\"city\":\"Santiago\"}", stompSession);
        }

        Thread.sleep(60000);
    }

}
