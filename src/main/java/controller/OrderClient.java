package controller;

import beanstalk.bigtable.CreateIfNotExists;
import beanstalk.data.types.*;
import beanstalk.values.GatewayHeader;
import beanstalk.values.Project;
import beanstalk.values.Table;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.http.annotation.Header;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import service.PubSubSubscription;

@ServerWebSocket("/stream")
public class OrderClient {

    private final WebSocketBroadcaster broadcaster;

    private final PubSubSubscription pubSubSubscription;

    public OrderClient(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
        pubSubSubscription = new PubSubSubscription(broadcaster);
    }

    @EventListener
    public void onStartupEvent(StartupEvent event) {

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.LIVE_ORDER, MessageOrder.class);

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.ORDER_BOOK, OrderBook.class);

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.ORDER_WAREHOUSE, Order.class);

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.ACCOUNT, Account.class);

        pubSubSubscription.start();
    }

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        System.out.println("Shutting down");
        pubSubSubscription.shutdown();
    }

    @OnOpen
    public void onOpen(WebSocketSession session, @Header(GatewayHeader.account) String accountID) {
        System.out.println(accountID);
        broadcaster.broadcastAsync("hello");
    }

    @OnMessage
    public void onMessage(String msg, WebSocketSession session) {
        System.out.println(msg);
        broadcaster.broadcastAsync(msg);
    }

    @OnClose
    public void onClose() {
        String msg = "[" + "] Disconnected!";
        System.out.println(msg);
    }

}
