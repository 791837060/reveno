package org.reveno.atp.acceptance.tests;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.reveno.atp.acceptance.api.commands.CreateNewAccountCommand;
import org.reveno.atp.acceptance.api.commands.NewOrderCommand;
import org.reveno.atp.acceptance.api.transactions.AcceptOrder;
import org.reveno.atp.acceptance.api.transactions.CreateAccount;
import org.reveno.atp.acceptance.api.transactions.Credit;
import org.reveno.atp.acceptance.api.transactions.Debit;
import org.reveno.atp.acceptance.handlers.Commands;
import org.reveno.atp.acceptance.handlers.Transactions;
import org.reveno.atp.acceptance.model.Account;
import org.reveno.atp.acceptance.model.Order;
import org.reveno.atp.acceptance.model.Order.OrderType;
import org.reveno.atp.acceptance.model.immutable.ImmutableAccount;
import org.reveno.atp.acceptance.model.immutable.ImmutableOrder;
import org.reveno.atp.acceptance.model.mutable.MutableAccount;
import org.reveno.atp.acceptance.model.mutable.MutableOrder;
import org.reveno.atp.acceptance.views.AccountView;
import org.reveno.atp.acceptance.views.OrderView;
import org.reveno.atp.api.ChannelOptions;
import org.reveno.atp.api.Configuration.CpuConsumption;
import org.reveno.atp.api.Configuration.ModelType;
import org.reveno.atp.api.Reveno;
import org.reveno.atp.api.commands.Result;
import org.reveno.atp.api.domain.WriteableRepository;
import org.reveno.atp.api.exceptions.FailoverRulesException;
import org.reveno.atp.api.transaction.TransactionInterceptor;
import org.reveno.atp.api.transaction.TransactionStage;
import org.reveno.atp.core.Engine;
import org.reveno.atp.test.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@RunWith(Parameterized.class)
public class RevenoBaseTest {

    protected static final Logger log = LoggerFactory.getLogger(RevenoBaseTest.class);
    @Parameter
    public ModelType modelType;
    protected volatile boolean dontDelete = false;
    protected File tempDir;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ModelType.IMMUTABLE}, {ModelType.MUTABLE}
        });
    }

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("protostuff.runtime.collection_schema_on_repeated_fields", "true");
    }

    @Before
    public void setUp() {
        tempDir = Files.createTempDir();
        log.info("Dir: " + tempDir);
    }

    @After
    public void tearDown() throws IOException {
        if (!dontDelete)
            FileUtils.delete(tempDir);
    }

    @Test
    public void mockTest() {
        // gradle requires such hack though
    }

    protected TestRevenoEngine createEngine() {
        return createEngine((r) -> {
        });
    }

    protected TestRevenoEngine createEngine(Consumer<TestRevenoEngine> consumer) {
        return createEngine(consumer, true);
    }

    protected TestRevenoEngine createEngine(Consumer<TestRevenoEngine> consumer, boolean meter) {
        TestRevenoEngine reveno = new TestRevenoEngine(tempDir);
        configure(reveno);
        txPerSecondMeter(reveno);
        consumer.accept(reveno);
        setModelType();

        return reveno;
    }

    protected void setModelType() {
        if (modelType == ModelType.IMMUTABLE) {
            Transactions.accountFactory = ImmutableAccount.FACTORY;
            Transactions.orderFactory = ImmutableOrder.FACTORY;
        } else {
            Transactions.accountFactory = MutableAccount.FACTORY;
            Transactions.orderFactory = MutableOrder.FACTORY;
        }
    }

    protected <T extends Engine> T configure(T reveno) {
        reveno.config().cpuConsumption(CpuConsumption.LOW);
        reveno.config().modelType(modelType);
        reveno.config().journaling().channelOptions(ChannelOptions.BUFFERING_OS);
        reveno.config().mapCapacity(16);
        reveno.config().mapLoadFactor(0.5f);

        reveno.domain().command(CreateNewAccountCommand.class, Long.class, Commands::createAccount);
        reveno.domain().command(NewOrderCommand.class, Long.class, Commands::newOrder);
        reveno.domain().command(Credit.class, Commands::credit);
        reveno.domain().command(Debit.class, Commands::debit);
        reveno.domain().transactionAction(CreateAccount.class, Transactions::createAccount);
        reveno.domain().transactionAction(AcceptOrder.class, Transactions::acceptOrder);
        reveno.domain().transactionAction(Credit.class, Transactions::credit);
        reveno.domain().transactionAction(Debit.class, Transactions::debit);

        reveno.domain().viewMapper(Account.class, AccountView.class, (id, e, r) ->
                new AccountView(id, e.currency(), e.balance(), e.orders(), reveno.query()));
        reveno.domain().viewMapper(Order.class, OrderView.class, (id, e, r) ->
                new OrderView(id, e.accountId(), e.size(), e.price(), e.time(), e.positionId(),
                        e.symbol(), e.orderStatus(), e.orderType(), reveno.query()));
        return reveno;
    }

    protected void txPerSecondMeter(TestRevenoEngine reveno) {
        reveno.interceptors().add(TransactionStage.TRANSACTION, new TransactionInterceptor() {
            protected long start = -1L;

            @Override
            public void intercept(long transactionId, long time, long flag, WriteableRepository repository,
                                  TransactionStage stage) {
                if (flag == 0) {
                    if (start == -1 || transactionId == 1) start = System.currentTimeMillis();
                    if (transactionId % 10_000_000 == 0) {
                        log.info(String.format("%s tx per second!", (
                                Math.round((double) 10_000_000 * 1000) / (System.currentTimeMillis() - start))));
                        start = System.currentTimeMillis();
                    }
                }
            }

            @Override
            public void destroy() {
            }
        });
    }

    protected int generateAndSendCommands(Reveno reveno, int count)
            throws InterruptedException, ExecutionException {
        return generateAndSendCommands(reveno, count, i -> {
        });
    }

    protected int generateAndSendCommands(Reveno reveno, int count, Consumer<Integer> c)
            throws InterruptedException, ExecutionException {
        sendCommandsBatch(reveno, new CreateNewAccountCommand("USD", 1000_000L), count);

        List<NewOrderCommand> commands = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            commands.add(new NewOrderCommand(i + 1, null, "EUR/USD",
                    134000, (long) (1000 * Math.random()), OrderType.MARKET));
        }
        return sendCommandsBatch(reveno, commands, c);
    }

    protected File findFirstFile(String prefix) {
        return tempDir.listFiles((dir, name) -> name.startsWith(prefix))[0];
    }

    @SuppressWarnings("unchecked")
    protected <T> T sendCommandSync(Reveno reveno, Object command) throws InterruptedException, ExecutionException {
        Result<T> result = (Result<T>) reveno.executeCommand(command).get();
        if (!result.isSuccess())
            throw new RuntimeException(result.getException());
        return result.getResult();
    }

    protected int sendCommandsBatch(Reveno reveno, Object command, int n) throws InterruptedException, ExecutionException {
        int failedToSend = 0;
        for (int i = 0; i < n - 1; i++) {
            try {
                reveno.executeCommand(command);
            } catch (FailoverRulesException e) {
                failedToSend++;
                log.info("Skipping to send command {}, message: {}", i, e.getMessage());
                LockSupport.parkNanos(100);
            }
        }
        try {
            sendCommandSync(reveno, command);
        } catch (FailoverRulesException e) {
            failedToSend++;
            log.info("Skipping to send command {}, message: {}", n, e.getMessage());
        }
        return failedToSend;
    }

    protected void sendCommandsBatch(Reveno reveno, List<?> commands) throws InterruptedException, ExecutionException {
        sendCommandsBatch(reveno, commands, i -> {
        });
    }

    protected int sendCommandsBatch(Reveno reveno, List<?> commands, Consumer<Integer> c) throws InterruptedException, ExecutionException {
        AtomicInteger failedToSend = new AtomicInteger(0);
        List<CompletableFuture<Result<Object>>> futures = new ArrayList<>(commands.size());
        for (int i = 0; i < commands.size() - 1; i++) {
            c.accept(i);
            try {
                futures.add(reveno.executeCommand(commands.get(i)));
            } catch (Throwable e) {
                failedToSend.incrementAndGet();
                log.info("Skipping to send command {}, message: {}", i, e.getMessage());
                LockSupport.parkNanos(100);
            }
        }
        try {
            sendCommandSync(reveno, commands.get(commands.size() - 1));
        } catch (Throwable e) {
            failedToSend.incrementAndGet();
            log.info("Skipping to send command {}, message: {}", commands.size(), e.getMessage());
        }
        futures.forEach(f -> {
            try {
                if (!f.get().isSuccess()) {
                    failedToSend.incrementAndGet();
                }
            } catch (Throwable ignored) {
            }
        });
        return failedToSend.get();
    }

    protected <T> Waiter listenFor(Reveno reveno, Class<T> event) {
        Waiter waiter = new Waiter(1);
        reveno.events().eventHandler(event, (e, m) -> waiter.countDown());
        return waiter;
    }

    protected <T> Waiter listenAsyncFor(Reveno reveno, Class<T> event, int n) {
        return listenAsyncFor(reveno, event, n, (o) -> {
        });
    }

    protected <T> Waiter listenAsyncFor(Reveno reveno, Class<T> eventType, int n, Consumer<Long> c) {
        Waiter waiter = new Waiter(n);
        reveno.events().asyncEventHandler(eventType, (e, m) -> {
            long count = 0;
            // since might be N threads executing this, count and countDown operations are not in sync,
            // thus some numbers are skipped because of contention
            synchronized (waiter) {
                waiter.countDown();
                count = waiter.getCount();
            }
            c.accept(count);
        });
        return waiter;
    }

    protected <T> Waiter listenFor(Reveno reveno, Class<T> event, int n) {
        return listenFor(reveno, event, n, (a) -> {
        });
    }

    protected <T> Waiter listenFor(Reveno reveno, Class<T> event, int n, Consumer<Long> c) {
        Waiter waiter = new Waiter(n);
        reveno.events().eventHandler(event, (e, m) -> {
            waiter.countDown();
            c.accept(waiter.getCount());
        });
        return waiter;
    }

    protected void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Waiter extends CountDownLatch {

        public Waiter(int count) {
            super(count);
        }

        public boolean isArrived() throws InterruptedException {
            return await(10, TimeUnit.SECONDS);
        }

        public boolean isArrived(int time) throws InterruptedException {
            return await(time, TimeUnit.SECONDS);
        }

        public void awaitSilent() {
            try {
                isArrived();
            } catch (InterruptedException e) {
            }
        }

    }

}
