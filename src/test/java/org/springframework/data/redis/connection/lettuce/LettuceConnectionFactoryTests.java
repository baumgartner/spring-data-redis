package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;

import com.lambdaworks.redis.RedisAsyncConnection;

public class LettuceConnectionFactoryTests {

	private LettuceConnectionFactory factory;

	private StringRedisConnection connection;

	@Before
	public void setUp() {
		factory = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory.afterPropertiesSet();
		connection = new DefaultStringRedisConnection(factory.getConnection());
	}

	@After
	public void tearDown() {
		factory.destroy();
	}

	@Test
	public void testGetNewConnectionOnError() throws Exception {
		factory.setValidateConnection(true);
		connection.lPush("alist", "baz");
		RedisAsyncConnection nativeConn = (RedisAsyncConnection) connection.getNativeConnection();
		nativeConn.close();
		// Give some time for async channel close
		Thread.sleep(500);
		connection.bLPop(1, "alist".getBytes());
		try {
			connection.get("test3");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, shared conn is closed
		}
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				factory.getConnection());
		assertNotSame(nativeConn, conn2.getNativeConnection());
		conn2.set("anotherkey", "anothervalue");
		assertEquals("anothervalue", conn2.get("anotherkey"));
	}

	@Test
	public void testConnectionErrorNoValidate() throws Exception {
		connection.lPush("ablist", "baz");
		((RedisAsyncConnection) connection.getNativeConnection()).close();
		// Give some time for async channel close
		Thread.sleep(500);
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				factory.getConnection());
		try {
			conn2.set("anotherkey", "anothervalue");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, as we are re-using the natively closed conn
		}
	}

	@Test
	public void testValidateNoError() {
		factory.setValidateConnection(true);
		RedisConnection conn2 = factory.getConnection();
		assertSame(connection.getNativeConnection(), conn2.getNativeConnection());
	}

	@Test
	public void testSelectDb() {
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection connection2 = new DefaultStringRedisConnection(factory2.getConnection());
		connection2.flushDb();
		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");
		try {
			// there should still be nothing in database 1
			assertEquals(Long.valueOf(0),connection2.dbSize());
		} finally {
			factory2.destroy();
		}
	}
}
