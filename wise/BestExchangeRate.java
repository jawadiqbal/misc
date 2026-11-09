import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TransferWise "Trading Routes": given directed exchange rates, find the best rate from A to B,
 * possibly via intermediaries. The reverse rate is not the inverse. No cycle multiplies to more than 1.
 *
 * Bellman-Ford on max product: relax every edge V-1 times. O(V * E).
 * No profitable cycles means the best route is a simple path, so V-1 rounds are enough.
 *
 * Run: java BestExchangeRate.java
 */
public class BestExchangeRate {

    record Rate(String from, String to, double rate) {}

    /** Returns the best achievable rate, or -1 if B is unreachable. */
    static double best(String from, String to, List<Rate> rates) {
        if (from.equals(to)) return 1.0;
        Map<String, Double> best = new HashMap<>();
        best.put(from, 1.0);
        int currencies = (int) rates.stream().flatMap(r -> List.of(r.from(), r.to()).stream()).distinct().count();

        for (int round = 0; round < currencies - 1; round++) {
            boolean changed = false;
            for (Rate r : rates) {
                Double atFrom = best.get(r.from());
                if (atFrom == null) continue;
                double candidate = atFrom * r.rate();
                if (candidate > best.getOrDefault(r.to(), -1.0)) {
                    best.put(r.to(), candidate);
                    changed = true;
                }
            }
            if (!changed) break;
        }
        return best.getOrDefault(to, -1.0);
    }

    /** Parses the contest format: "A B", N, then N lines "SRC DST RATE". */
    static double solve(String input) {
        String[] lines = input.strip().split("\\R");
        String[] pair = lines[0].trim().split("\\s+");
        int n = Integer.parseInt(lines[1].trim());
        List<Rate> rates = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String[] p = lines[2 + i].trim().split("\\s+");
            rates.add(new Rate(p[0], p[1], Double.parseDouble(p[2])));
        }
        return best(pair[0], pair[1], rates);
    }

    public static void main(String[] args) {
        String input = """
                PKR KRW
                4
                PKR USD 0.0036
                USD KRW 1380.0
                PKR GBP 0.0028
                GBP KRW 1760.0
                """;
        check(close(solve(input), 0.0036 * 1380.0), "picks the better of two routes");

        List<Rate> rates = List.of(
                new Rate("EUR", "USD", 1.10),
                new Rate("USD", "JPY", 150.0),
                new Rate("EUR", "JPY", 160.0),
                new Rate("JPY", "EUR", 0.006));
        check(close(best("EUR", "JPY", rates), 165.0), "indirect route beats direct");
        check(close(best("JPY", "USD", rates), 0.006 * 1.10), "multi-hop");
        check(best("USD", "GBP", rates) == -1, "unreachable");
        check(best("EUR", "EUR", rates) == 1.0, "same currency");

        System.out.println("BestExchangeRate: all checks passed");
    }

    static boolean close(double a, double b) { return Math.abs(a - b) < 1e-9 * Math.max(1, Math.abs(b)); }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
