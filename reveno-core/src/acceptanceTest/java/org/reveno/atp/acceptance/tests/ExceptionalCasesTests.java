package org.reveno.atp.acceptance.tests;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.reveno.atp.acceptance.api.commands.CreateNewAccountCommand;
import org.reveno.atp.acceptance.api.commands.NewOrderCommand;
import org.reveno.atp.acceptance.api.events.AccountCreatedEvent;
import org.reveno.atp.acceptance.api.events.OrderCreatedEvent;
import org.reveno.atp.acceptance.model.Order.OrderType;
import org.reveno.atp.acceptance.views.AccountView;
import org.reveno.atp.acceptance.views.OrderView;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExceptionalCasesTests extends RevenoBaseTest {

	@Test
	public void testEventsCommitsFileCorrupted() throws Exception {
		TestRevenoEngine reveno = createEngine();
		try {
			reveno.startup();

			generateAndSendCommands(reveno, 5_000);

			Assert.assertEquals(5_000, reveno.query().select(AccountView.class).size());
			Assert.assertEquals(5_000, reveno.query().select(OrderView.class).size());

			reveno.shutdown();

			eraseRandomBuffer(findFirstFile("evn"));

			reveno = createEngine();
			Waiter orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class, 2000);
			reveno.startup();

			Assert.assertEquals(5_000, reveno.query().select(AccountView.class).size());
			Assert.assertEquals(5_000, reveno.query().select(OrderView.class).size());

			Assert.assertTrue(orderCreatedEvent.isArrived(10));

			reveno.shutdown();

			reveno = createEngine();
			Waiter accountCreatedEvent = listenFor(reveno, AccountCreatedEvent.class, 1);
			orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class, 1);
			reveno.startup();

			Assert.assertEquals(5_000, reveno.query().select(AccountView.class).size());
			Assert.assertEquals(5_000, reveno.query().select(OrderView.class).size());

			try {
				Assert.assertFalse(accountCreatedEvent.isArrived());
				Assert.assertFalse(orderCreatedEvent.isArrived());
				Assert.assertEquals(1, accountCreatedEvent.getCount());
				Assert.assertEquals(1, orderCreatedEvent.getCount());
			} catch (Throwable t) {
				Logger.getLogger(ExceptionalCasesTests.class).info(tempDir.getAbsolutePath());
				dontDelete = true;
				throw new RuntimeException(t);
			}
		} finally {
			reveno.shutdown();
		}
	}
	
	@Test
	public void testTransactionCommitsFileCorrupted() throws Exception {
		TestRevenoEngine reveno = createEngine();
		try {
			reveno.startup();

			generateAndSendCommands(reveno, 5_000);

			reveno.syncAll();
			reveno.shutdown();

			eraseRandomBuffer(findFirstFile("tx"));
			eraseRandomBuffer(findFirstFile("evn"));

			reveno = createEngine();
			reveno.startup();

			Assert.assertTrue(5_000 > reveno.query().select(OrderView.class).size());

			Collection<AccountView> accs = reveno.query().select(AccountView.class, a -> a.accountId == 1);

			if (accs.size() != 0) {
				Assert.assertEquals(accs.size(), 1);
				Waiter orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class, 1);
				AccountView acc = accs.iterator().next();
				long orderId = sendCommandSync(reveno, new NewOrderCommand(acc.accountId, null,
						"TEST/TEST", 134000, 1000, OrderType.MARKET));
				acc = reveno.query().find(AccountView.class, acc.accountId);
				LOG.info("Account orders: " + acc.orders().size() + ", " + acc.orders.size());
				Assert.assertTrue(acc.orders().stream().filter(o -> o.id == orderId).findFirst().isPresent());
				// in very rare cases we might get result, but event didn't came to pipe, so
				// even though we syncAll(), the event might come to pipe after syncAll().
				Assert.assertTrue(orderCreatedEvent.isArrived());
			}

			reveno.syncAll();
			reveno.shutdown();

			reveno = createEngine();
			Waiter orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class, 1);
			reveno.startup();

			if (accs.size() != 0) {
				AccountView acc = reveno.query().find(AccountView.class, 1L);
				Assert.assertTrue(acc.orders().stream().filter(o -> o.symbol.equals("TEST/TEST")).findFirst().isPresent());
			}

			try {
				Assert.assertFalse(orderCreatedEvent.isArrived());
			} catch (Throwable t) {
				Logger.getLogger(ExceptionalCasesTests.class).info(tempDir.getAbsolutePath());
				dontDelete = true;
				throw new RuntimeException(t);
			}
		} finally {
			reveno.shutdown();
		}
	}

	@Test
	public void testNoPartialReplayOfCommandBatch() throws Exception {
		TestRevenoEngine reveno = createEngine();
		try {
			reveno.startup();

			sendCommandSync(reveno, new CreateNewAccountCommand("USD", 1000_000L));

			List<Object> commands = new ArrayList<>();
			commands.add(new CreateNewAccountCommand("USD", 1000_000L));

			for (int i = 0; i < 10; i++) {
				commands.add(new NewOrderCommand(2L, null, "EUR/USD", 134000, 1000, OrderType.MARKET));
			}

			reveno.performCommands(commands).get();

			AccountView account = reveno.query().find(AccountView.class, 1L);
			Assert.assertEquals(0, account.orders().size());
			account = reveno.query().find(AccountView.class, 2L);
			Assert.assertEquals(10, account.orders().size());

			reveno.shutdown();

			eraseBuffer(findFirstFile("tx"), 0.9);

			reveno = createEngine();
			reveno.startup();

			Assert.assertTrue(reveno.query().findO(AccountView.class, 1L).isPresent());
			Assert.assertFalse(reveno.query().findO(AccountView.class, 2L).isPresent());
		} finally {
			reveno.shutdown();
		}
	}

	protected void eraseRandomBuffer(File file) throws Exception {
		eraseBuffer(file, 0.8);
	}

	protected void eraseBuffer(File file, double ratio) throws Exception {
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			raf.setLength((long)((raf.length() * ratio)));
		}
	}

	protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ExceptionalCasesTests.class);
}
