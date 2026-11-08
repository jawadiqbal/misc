import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Order execution against balance bounds: an account holds balances in several currencies, and an
 * order converts an amount of one currency into another at a quoted rate. Reject if the source
 * balance can't cover it. Never go negative, never apply the same order twice.
 *
 * Money is BigDecimal with an explicit scale. Each account is locked while an order runs, so
 * concurrent orders on one account can't both pass the balance check.
 *
 * Run: java OrderExecution.java
 */
public class OrderExecution {

    enum Status { FILLED, INSUFFICIENT_FUNDS, UNKNOWN_ACCOUNT }

    record Order(String orderId, String accountId, String sellCurrency, BigDecimal sellAmount,
                 String buyCurrency, BigDecimal rate) {}

    record Execution(String orderId, Status status, BigDecimal debited, BigDecimal credited) {}

    static final class Account {
        final String id;
        private final Map<String, BigDecimal> balances = new HashMap<>();

        Account(String id) { this.id = id; }

        synchronized BigDecimal balance(String currency) { return balances.getOrDefault(currency, BigDecimal.ZERO); }

        synchronized void deposit(String currency, BigDecimal amount) { balances.merge(currency, amount, BigDecimal::add); }
    }

    static final class ExecutionEngine {
        private static final int SCALE = 2;
        private final Map<String, Account> accounts = new ConcurrentHashMap<>();
        private final Map<String, Execution> executed = new ConcurrentHashMap<>();

        Account open(String accountId) { return accounts.computeIfAbsent(accountId, Account::new); }

        Execution execute(Order o) {
            if (o.sellAmount().signum() <= 0 || o.rate().signum() <= 0) {
                throw new IllegalArgumentException("amount and rate must be positive");
            }
            return executed.computeIfAbsent(o.orderId(), id -> apply(o));
        }

        private Execution apply(Order o) {
            Account account = accounts.get(o.accountId());
            if (account == null) return new Execution(o.orderId(), Status.UNKNOWN_ACCOUNT, BigDecimal.ZERO, BigDecimal.ZERO);

            BigDecimal debit = o.sellAmount().setScale(SCALE, RoundingMode.HALF_EVEN);
            // Round the credit down: the customer never receives more than the quoted rate pays for.
            BigDecimal credit = o.sellAmount().multiply(o.rate()).setScale(SCALE, RoundingMode.DOWN);
            synchronized (account) {
                if (account.balance(o.sellCurrency()).compareTo(debit) < 0) {
                    return new Execution(o.orderId(), Status.INSUFFICIENT_FUNDS, BigDecimal.ZERO, BigDecimal.ZERO);
                }
                account.deposit(o.sellCurrency(), debit.negate());
                account.deposit(o.buyCurrency(), credit);
            }
            return new Execution(o.orderId(), Status.FILLED, debit, credit);
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEngine engine = new ExecutionEngine();
        Account a = engine.open("acc-1");
        a.deposit("GBP", new BigDecimal("100.00"));

        Order o1 = new Order("o1", "acc-1", "GBP", new BigDecimal("60.00"), "EUR", new BigDecimal("1.1734"));
        Execution e1 = engine.execute(o1);
        check(e1.status() == Status.FILLED && e1.credited().equals(new BigDecimal("70.40")), "fill, credit rounded down");
        check(a.balance("GBP").equals(new BigDecimal("40.00")), "debited");
        check(engine.execute(o1) == e1 && a.balance("GBP").equals(new BigDecimal("40.00")), "same order id is not re-applied");

        Order o2 = new Order("o2", "acc-1", "GBP", new BigDecimal("40.01"), "USD", new BigDecimal("1.27"));
        check(engine.execute(o2).status() == Status.INSUFFICIENT_FUNDS, "cannot overdraw");
        check(engine.execute(new Order("o3", "nope", "GBP", BigDecimal.ONE, "EUR", BigDecimal.ONE)).status()
                == Status.UNKNOWN_ACCOUNT, "unknown account");

        Account b = engine.open("acc-2");
        b.deposit("USD", new BigDecimal("1000.00"));
        ExecutorService pool = Executors.newFixedThreadPool(16);
        AtomicInteger filled = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(50);
        for (int i = 0; i < 50; i++) {
            String id = "c" + i;
            pool.submit(() -> {
                Order o = new Order(id, "acc-2", "USD", new BigDecimal("30.00"), "EUR", new BigDecimal("0.92"));
                if (engine.execute(o).status() == Status.FILLED) filled.incrementAndGet();
                done.countDown();
            });
        }
        done.await();
        pool.shutdown();
        check(filled.get() == 33 && b.balance("USD").equals(new BigDecimal("10.00")), "concurrent orders respect the balance");

        System.out.println("OrderExecution: all checks passed");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
