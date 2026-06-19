package taxi;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class Taxi implements Runnable {

    private static final String[] LOC = {
            "CENTAR", "SIRI_CENTAR", "SEVER", "JUG", "ISTOK", "ZAPAD", "AERODROM"
    };

    private final String id;
    private final String kategorija;
    private volatile String lokacija;
    private final AtomicBoolean busy = new AtomicBoolean(false);
    private final Random rnd = new Random();

    private MqttHelper mqtt;
    private String currentSub;  // trenutni topic filter na koji smo pretplaćeni

    public Taxi(String id, String kategorija, String lokacija) {
        this.id = id;
        this.kategorija = kategorija;
        this.lokacija = lokacija;
    }

    @Override
    public void run() {
        try {
            mqtt = new MqttHelper("taxi-" + id);

            resubscribe();

            mqtt.subscribe("taxi/taxi/" + id, (t, msg) -> {
                if (msg == null) return;

                switch (msg.status == null ? "" : msg.status) {
                    case "ACCEPTED" -> {
                        busy.set(true);

                        System.out.println("[TAXI " + id + "] ACCEPTED vožnja req="
                                + msg.requestId.substring(0, 8)
                                + " " + msg.polazak + "->" + msg.odrediste);

                        drive(msg);
                    }

                    case "REJECTED" -> {
                        busy.set(false);

                        System.out.println("[TAXI " + id + "] ponuda ODBIJENA req="
                                + (msg.requestId != null ? msg.requestId.substring(0, 8) : "?"));
                    }

                    default -> {}
                }
            });

            Thread.sleep(Long.MAX_VALUE);

        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            System.err.println("[TAXI " + id + "] greška: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private synchronized void resubscribe() {
        String newSub = "taxi/requests/" + lokacija + "/" + kategorija + "/#";

        if (newSub.equals(currentSub)) return;

        if (currentSub != null) {
            mqtt.unsubscribe(currentSub);
        }

        currentSub = newSub;

        mqtt.subscribe(newSub, (topic, req) -> {
            if (req == null) return;
            if (!"REQUEST".equals(req.status)) return;

            // Zauzet postaje tek kada dispečer prihvati baš njegovu ponudu
            if (busy.get()) return;

            sendOffer(req);
        });

        System.out.println("[TAXI " + id + "] (" + kategorija + ") pretplaćen na: " + newSub);
    }

    private void sendOffer(RideRequest req) {
        RideRequest o = new RideRequest();

        o.requestId  = req.requestId;
        o.customerId = req.customerId;
        o.taxiId     = id;
        o.kategorija = kategorija;
        o.polazak    = req.polazak;
        o.odrediste  = req.odrediste;
        o.duzina     = req.duzina;
        o.status     = "OFFER";

        o.eta = "AERODROM".equals(req.duzina)
                ? 5 + rnd.nextInt(20)
                : 1 + rnd.nextInt(10);

        double base = switch (kategorija) {
            case "PREMIUM" -> 300;
            case "MID"     -> 150;
            default        -> 80;
        };

        double duzinaMult = switch (req.duzina == null ? "" : req.duzina) {
            case "KRATKA"   -> 1.0 + rnd.nextDouble() * 0.5;
            case "SREDNJA"  -> 1.5 + rnd.nextDouble() * 1.0;
            case "DUZA"     -> 2.5 + rnd.nextDouble() * 2.0;
            case "AERODROM" -> 5.0 + rnd.nextDouble() * 3.0;
            default         -> 1.0 + rnd.nextDouble();
        };

        o.cena = base * duzinaMult;

        try {
            Thread.sleep(20 + rnd.nextInt(50));
        } catch (InterruptedException ignored) {}

        mqtt.publish("taxi/offers/" + req.requestId, o);

        System.out.println("[TAXI " + id + "] ponuda req=" + req.requestId.substring(0, 8)
                + " eta=" + o.eta + "min cena=" + String.format("%.0f", o.cena));
    }

    private void drive(RideRequest msg) {
        new Thread(() -> {
            try {
                long trajanje = 400;
                Thread.sleep(trajanje);

                lokacija = msg.odrediste;

                System.out.println("[TAXI " + id + "] završio vožnju, nova lokacija=" + lokacija);

                resubscribe();

                busy.set(false);

            } catch (InterruptedException ignored) {}
        }, "taxi-drive-" + id).start();
    }

    public static void main(String[] args) throws InterruptedException {
        String[] kat = {"ECONOMY", "MID", "PREMIUM"};
        Random r = new Random();

        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100;

        System.out.println("[TAXI-MAIN] Pokrećem " + count + " taksi vozila...");

        for (int i = 0; i < count; i++) {
            String kategorija = kat[r.nextInt(3)];
            String lokacija   = LOC[r.nextInt(LOC.length)];

            new Thread(new Taxi("T" + i, kategorija, lokacija)).start();

            Thread.sleep(10);
        }
    }
}