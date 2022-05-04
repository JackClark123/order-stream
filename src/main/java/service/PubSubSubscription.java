package service;

import beanstalk.bigtable.Beanstalk;
import beanstalk.data.BeanstalkData;
import beanstalk.data.types.Account;
import beanstalk.data.types.MessageOrder;
import beanstalk.values.Project;
import beanstalk.values.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.*;
import io.micronaut.http.HttpResponse;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;

public class PubSubSubscription implements MessageReceiver {

    private final WebSocketBroadcaster broadcaster;

    private Subscriber subscriber;

    private final ObjectMapper objectMapper;

    private BigtableDataClient dataClient;

    public PubSubSubscription(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;

        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JodaModule());

        String subscription = System.getenv("SUBSCRIPTION");

        Subscriber.Builder builder =
                Subscriber.newBuilder(subscription, this);

        FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                        // 1,000 outstanding messages. Must be >0. It controls the maximum number of messages
                        // the subscriber receives before pausing the message stream.
                        .setMaxOutstandingElementCount(1000L)
                        // 100 MiB. Must be >0. It controls the maximum size of messages the subscriber
                        // receives before pausing the message stream.
                        .setMaxOutstandingRequestBytes(100L * 1024L * 1024L)
                        .build();

        try {
            builder = builder.setFlowControlSettings(flowControlSettings);
            this.subscriber = builder.build();
        } catch (Exception e) {
            System.out.println("Could not create subscriber: " + e);
            System.exit(1);
        }

        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(Project.PROJECT).setInstanceId(Table.INSTANCE).build();

        // Creates a bigtable data client.
        try {
            dataClient = BigtableDataClient.create(settings);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void start() {
        subscriber.startAsync().awaitRunning();
    }

    public void shutdown() {
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        String msg = message.getData().toStringUtf8();

        consumer.ack();

        System.out.println(msg);

        MessageOrder messageOrder = null;

        try {
            messageOrder = objectMapper.readValue(msg, MessageOrder.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        if (messageOrder != null && messageOrder.getOrder().getIdentifier() != null) {
            broadcaster.broadcastAsync(msg, isValid(messageOrder.getOrder().getAccountID()));

            Account account = Beanstalk.account(dataClient, messageOrder.getOrder().getAccountID());

            broadcaster.broadcastAsync(account, isValid(messageOrder.getOrder().getAccountID()));

        }
    }

    private Predicate<WebSocketSession> isValid(String accountId) {
        return s -> accountId.equalsIgnoreCase(s.getUriVariables().get("account", String.class, null));
    }

}
