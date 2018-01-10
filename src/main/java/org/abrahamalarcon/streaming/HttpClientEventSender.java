package org.abrahamalarcon.streaming;

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

import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class HttpClientEventSender
{
    private static Logger logger = Logger.getLogger(HttpClientEventSender.class.getName());
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
        HttpClientEventSender client = new HttpClientEventSender();

        ListenableFuture<StompSession> f = client.connect();
        StompSession stompSession = f.get();

        String clientId = "client1", eventId = "geolookup";

        //send 20 new events to feed stream
        for(int i=0; i<20; i++)
        {
            String endpoint = String.format("/app/newevent/%s/%s/%s", clientId, eventId, i);
            logger.info(String.format("Sending message to %s", endpoint) + stompSession);
            client.send(endpoint, "{\"country\":\"Chile\",\"city\":\"Santiago\"}", stompSession);
        }
    }

}
