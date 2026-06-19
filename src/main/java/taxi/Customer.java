package taxi;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Customer implements Runnable {

    private static final String[] LOC = {
            "CENTAR", "SIRI_CENTAR", "SEVER", "JUG", "ISTOK", "ZAPAD", "AERODROM"
    };

    private static final String[] KRIT = {"FASTEST", "CHEAPEST", "LUXURY"};

    private final String id;
    private final Random rnd = new Random();

    public Customer(String id) {
        this.id = id;
    }

    @Override
    public void run() {
        try {
            MqttHelper mqtt = new MqttHelper("customer-" + id);

            RideRequest req = new RideRequest();

            req.requestId  = UUID.randomUUID().toString();
            req.customerId = id;
            req.polazak    = LOC[rnd.nextInt(LOC.length)];

            do {
                req.odrediste = LOC[rnd.nextInt(LOC.length)];
            } while (req.odrediste.equals(req.polazak));

            req.kriterijum = KRIT[rnd.nextInt(KRIT.length)];
            req.duzina     = computeDuzina(req.polazak, req.odrediste);
            req.status     = "REQUEST";
            req.timestamp  = System.currentTimeMillis();

            CountDownLatch offerLatch = new CountDownLatch(1);
            final RideRequest[] offer = new RideRequest[1];

            mqtt.subscribe("taxi/customer/" + id, (t, msg) -> {
                if (msg != null
                        && msg.requestId != null
                        && msg.requestId.equals(req.requestId)) {
                    offer[0] = msg;
                    offerLatch.countDown();
                }
            });

            String publishTopic = "taxi/client/requests/" + req.polazak + "/" + req.duzina;

            mqtt.publish(publishTopic, req);

            System.out.println("[CUST " + id + "] zahtev " + req.requestId.substring(0, 8)
                    + " " + req.polazak + "->" + req.odrediste
                    + " krit=" + req.kriterijum
                    + " duzina=" + req.duzina);

            if (offerLatch.await(40, TimeUnit.SECONDS) && offer[0] != null) {
                RideRequest ack = offer[0];

                ack.status = "CUSTOMER_ACK";

                mqtt.publish("taxi/customer/ack/" + req.requestId, ack);

                System.out.println("[CUST " + id + "] PRIHVATIO ponudu taxi="
                        + ack.taxiId
                        + " cena=" + String.format("%.0f", ack.cena)
                        + " eta=" + ack.eta + "min");
            } else {
                System.out.println("[CUST " + id + "] NIJE dobio ponudu na vreme (timeout 40s)");
            }

            Thread.sleep(2000);

            mqtt.disconnect();

        } catch (Exception e) {
            System.err.println("[CUST " + id + "] greška: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String computeDuzina(String a, String b) {
        if ("AERODROM".equals(a) || "AERODROM".equals(b)) {
            return "AERODROM";
        }

        if (("CENTAR".equals(a) && "SIRI_CENTAR".equals(b))
                || ("SIRI_CENTAR".equals(a) && "CENTAR".equals(b))) {
            return "KRATKA";
        }

        if (("SEVER".equals(a) && "JUG".equals(b))
                || ("JUG".equals(a) && "SEVER".equals(b))
                || ("ISTOK".equals(a) && "ZAPAD".equals(b))
                || ("ZAPAD".equals(a) && "ISTOK".equals(b))) {
            return "DUZA";
        }

        return rnd.nextBoolean() ? "KRATKA" : "SREDNJA";
    }

    public static void main(String[] args) throws InterruptedException {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100;

        System.out.println("[CUST-MAIN] Pokrećem " + count + " mušterija...");

        for (int i = 0; i < count; i++) {
            new Thread(new Customer("C" + i)).start();

            Thread.sleep(1500);
        }
    }
}