package taxi;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class Dispatcher {

    private static final long OFFER_TIMEOUT_MS = 3000;
    private static final long CUSTOMER_ACK_TIMEOUT_MS = 10000;

    private final MqttHelper mqtt;

    private final Map<String, List<RideRequest>> offers = new ConcurrentHashMap<>();
    private final Map<String, RideRequest> originalReq = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> ackLatches = new ConcurrentHashMap<>();
    private final Map<String, RideRequest> customerAcks = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);

    private final List<RideRequest> completed = Collections.synchronizedList(new ArrayList<>());

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Dispatcher() throws Exception {
        mqtt = new MqttHelper("dispatcher-main");

        Thread.sleep(500);

        mqtt.subscribe("taxi/client/requests/#", (topic, req) -> {
            if (req == null) return;
            if (!"REQUEST".equals(req.status)) return;
            if (req.requestId == null) return;

            if (originalReq.putIfAbsent(req.requestId, req) != null) return;

            scheduler.submit(() -> handleRequest(req));
        });

        mqtt.subscribe("taxi/offers/#", (topic, off) -> {
            if (off == null || off.requestId == null) return;

            offers.computeIfAbsent(
                    off.requestId,
                    k -> Collections.synchronizedList(new ArrayList<>())
            ).add(off);
        });

        mqtt.subscribe("taxi/customer/ack/#", (topic, ack) -> {
            if (ack == null || ack.requestId == null) return;

            customerAcks.put(ack.requestId, ack);

            CountDownLatch latch = ackLatches.get(ack.requestId);
            if (latch != null) {
                latch.countDown();
            }
        });

        System.out.println("[DISP] pokrenut – čeka zahteve...");
    }

    private void handleRequest(RideRequest req) {
        System.out.println("[DISP] novi zahtev " + req.requestId.substring(0, 8)
                + " " + req.polazak + "->" + req.odrediste
                + " krit=" + req.kriterijum
                + " duzina=" + req.duzina);

        for (String kat : new String[]{"ECONOMY", "MID", "PREMIUM"}) {
            String t = "taxi/requests/" + req.polazak + "/" + kat + "/" + req.duzina;
            mqtt.publish(t, req);
        }

        scheduler.schedule(() -> {
            try {
                pickBest(req);
            } catch (Throwable t) {
                System.err.println("[DISP ERROR] " + t);
                t.printStackTrace();
            }
        }, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void pickBest(RideRequest req) {
        List<RideRequest> rawList = offers.get(req.requestId);

        if (rawList == null || rawList.isEmpty()) {
            System.out.println("[DISP] nema ponuda za zahtev: " + req.requestId.substring(0, 8));
            originalReq.remove(req.requestId);
            offers.remove(req.requestId);
            return;
        }

        List<RideRequest> list;

        synchronized (rawList) {
            list = new ArrayList<>(rawList);
        }

        Comparator<RideRequest> cmp = switch (req.kriterijum) {
            case "CHEAPEST" -> Comparator.comparingDouble(o -> o.cena);
            case "FASTEST"  -> Comparator.comparingInt(o -> o.eta);
            case "LUXURY"   -> Comparator.comparingInt(o ->
                    "PREMIUM".equals(o.kategorija) ? 0 :
                            "MID".equals(o.kategorija) ? 1 : 2);
            default -> Comparator.comparingInt(o -> o.eta);
        };

        RideRequest best = list.stream().min(cmp).orElse(null);

        if (best == null) {
            System.out.println("[DISP] nema validne ponude za zahtev: " + req.requestId.substring(0, 8));
            originalReq.remove(req.requestId);
            offers.remove(req.requestId);
            return;
        }

        best.status = "OFFER";

        mqtt.publish("taxi/customer/" + req.customerId, best);

        System.out.println("[DISP] -> ponuda mušteriji " + req.customerId
                + " taxi=" + best.taxiId
                + " cena=" + String.format("%.0f", best.cena)
                + " eta=" + best.eta);

        CountDownLatch ackLatch = new CountDownLatch(1);
        ackLatches.put(req.requestId, ackLatch);

        scheduler.submit(() -> waitForCustomerAck(req, best, list, ackLatch));
    }

    private void waitForCustomerAck(RideRequest req, RideRequest best,
                                    List<RideRequest> allOffers, CountDownLatch latch) {
        boolean acked = false;

        try {
            acked = latch.await(CUSTOMER_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (acked && customerAcks.containsKey(req.requestId)) {
            RideRequest accepted = new RideRequest();

            accepted.requestId  = req.requestId;
            accepted.taxiId     = best.taxiId;
            accepted.customerId = req.customerId;
            accepted.polazak    = req.polazak;
            accepted.odrediste  = req.odrediste;
            accepted.cena       = best.cena;
            accepted.eta        = best.eta;
            accepted.kategorija = best.kategorija;
            accepted.duzina     = req.duzina;
            accepted.status     = "ACCEPTED";
            accepted.timestamp  = System.currentTimeMillis();

            mqtt.publish("taxi/taxi/" + best.taxiId, accepted);

            System.out.println("[DISP] PRIHVAĆENO: taxi=" + best.taxiId
                    + " req=" + req.requestId.substring(0, 8));

            for (RideRequest o : allOffers) {
                if (o.taxiId != null && !o.taxiId.equals(best.taxiId)) {
                    RideRequest rej = new RideRequest();
                    rej.requestId = req.requestId;
                    rej.taxiId    = o.taxiId;
                    rej.status    = "REJECTED";
                    rej.timestamp = System.currentTimeMillis();

                    mqtt.publish("taxi/taxi/" + o.taxiId, rej);
                }
            }

            completed.add(accepted);
            logCompletedRide(accepted);

            System.out.println("[DISP] Ukupno završenih vožnji: " + completed.size());

        } else {
            System.out.println("[DISP] mušterija " + req.customerId
                    + " nije potvrdila req=" + req.requestId.substring(0, 8)
                    + " → REJECTED svima");

            for (RideRequest o : allOffers) {
                if (o.taxiId == null) continue;

                RideRequest rej = new RideRequest();
                rej.requestId = req.requestId;
                rej.taxiId    = o.taxiId;
                rej.status    = "REJECTED";
                rej.timestamp = System.currentTimeMillis();

                mqtt.publish("taxi/taxi/" + o.taxiId, rej);
            }
        }

        ackLatches.remove(req.requestId);
        customerAcks.remove(req.requestId);
        offers.remove(req.requestId);
        originalReq.remove(req.requestId);

        mqtt.clearRetained("taxi/customer/" + req.customerId);
    }

    private void logCompletedRide(RideRequest r) {
        String vreme = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(r.timestamp),
                ZoneId.systemDefault()
        ).format(FMT);

        String linija = String.format(
                "[VOŽNJA] taxi=%-6s  kat=%-8s  %s->%-12s  cena=%6.0f RSD  eta=%2dmin  vreme=%s  req=%s",
                r.taxiId,
                r.kategorija,
                r.polazak,
                r.odrediste,
                r.cena,
                r.eta,
                vreme,
                r.requestId.substring(0, 8)
        );

        System.out.println(linija);

        try (PrintWriter pw = new PrintWriter(new FileWriter("voznje.log", true))) {
            pw.println(linija);
        } catch (IOException e) {
            System.err.println("[DISP] Ne mogu da upišem u voznje.log: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        Dispatcher d = new Dispatcher();
        Thread.sleep(Long.MAX_VALUE);
    }
}