package taxi;

import com.google.gson.Gson;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class MqttHelper {

    private static final String BROKER = "tcp://localhost:1883";
    private static final Gson gson = new Gson();
    private final MqttClient client;
    private final List<String> topicFilters = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<BiConsumer<String, RideRequest>> callbacks = new java.util.concurrent.CopyOnWriteArrayList<>();

    public MqttHelper(String clientId) throws MqttException {
        String effectiveId = clientId;
        if (clientId.startsWith("taxi-") || clientId.startsWith("customer-")) {
            effectiveId = clientId + "-" + System.nanoTime() % 100000;
        }
        client = new MqttClient(BROKER, effectiveId, new MemoryPersistence());

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setAutomaticReconnect(false);
        opts.setConnectionTimeout(10);
        opts.setKeepAliveInterval(30);

        client.connect(opts);

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage msg) {
                try {
                    if (msg == null || msg.getPayload() == null || msg.getPayload().length == 0) {
                        return;
                    }

                    String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
                    RideRequest r = gson.fromJson(payload, RideRequest.class);

                    if (r == null) {
                        return;
                    }
                    for (int i = 0; i < topicFilters.size(); i++) {
                        if (topicMatches(topicFilters.get(i), topic)) {
                            callbacks.get(i).accept(topic, r);
                        }
                    }

                } catch (Exception e) {
                    System.err.println("[MQTT] greška: " + e.getMessage());
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("[MQTT] konekcija izgubljena: "
                        + (cause != null ? cause.getMessage() : "nepoznat razlog"));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
    }

    public void publish(String topic, RideRequest msg, boolean retained) {
        try {
            MqttMessage m = new MqttMessage(gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
            m.setQos(1);
            m.setRetained(retained);
            client.publish(topic, m);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void publish(String topic, RideRequest msg) {
        publish(topic, msg, false);
    }

    public void clearRetained(String topic) {
        try {
            MqttMessage m = new MqttMessage(new byte[0]);
            m.setRetained(true);
            client.publish(topic, m);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void subscribe(String topicFilter, BiConsumer<String, RideRequest> cb) {
        try {
            topicFilters.add(topicFilter);
            callbacks.add(cb);

            IMqttToken token = client.subscribeWithResponse(topicFilter, 1);
            token.waitForCompletion(2000);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void unsubscribe(String topicFilter) {
        try {
            int idx = topicFilters.indexOf(topicFilter);

            if (idx >= 0) {
                topicFilters.remove(idx);
                callbacks.remove(idx);
            }

            client.unsubscribe(topicFilter);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private boolean topicMatches(String filter, String topic) {
        String[] fParts = filter.split("/");
        String[] tParts = topic.split("/");

        for (int i = 0; i < fParts.length; i++) {
            if ("#".equals(fParts[i])) {
                return true;
            }
            if (i >= tParts.length) {
                return false;
            }
            if (!"+".equals(fParts[i]) && !fParts[i].equals(tParts[i])) {
                return false;
            }
        }

        return fParts.length == tParts.length;
    }

    public void disconnect() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}