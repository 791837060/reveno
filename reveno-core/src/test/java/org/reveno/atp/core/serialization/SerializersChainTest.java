package org.reveno.atp.core.serialization;

import org.junit.Assert;
import org.junit.Test;
import org.reveno.atp.core.api.serialization.TransactionInfoSerializer;
import org.reveno.atp.core.channel.ChannelBuffer;
import org.reveno.atp.core.engine.components.SerializersChain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SerializersChainTest {

	@Test
	public void test() {
		List<TransactionInfoSerializer> serializers = new ArrayList<>();
		serializers.add(new ProtostuffSerializer());
		serializers.add(new DefaultJavaSerializer());
		
		SerializersChain chain = new SerializersChain(serializers);
        ChannelBuffer buffer = new ChannelBuffer(java.nio.ByteBuffer.allocate(1024 * 1024));
		chain.registerTransactionType(User.class);
		
		User user = new User("Artem", 22);
		chain.serializeCommands(Arrays.asList(new Object[] { user }), buffer);

        buffer.getBuffer().flip();

		user = (User)chain.deserializeCommands(buffer).get(0);
		Assert.assertEquals("Artem", user.getName());
		Assert.assertEquals(22, user.getAge());
	}
	
	@Test
	public void testProtostuffFailJavaWin() {
		List<TransactionInfoSerializer> serializers = new ArrayList<>();
		serializers.add(new ProtostuffSerializer());
		serializers.add(new DefaultJavaSerializer());
		
		SerializersChain chain = new SerializersChain(serializers);
        ChannelBuffer buffer = new ChannelBuffer(java.nio.ByteBuffer.allocate(1024 * 1024));
		chain.registerTransactionType(Empty.class);
		chain.serializeCommands(Arrays.asList(new Object[] { new Empty() }), buffer);
        buffer.getBuffer().flip();
		Empty empty = (Empty)chain.deserializeCommands(buffer).get(0);
		
		Assert.assertNotNull(empty);
	}
	
	public static class FullyEmpty {}
	@SuppressWarnings("serial")
	public static class Empty implements Serializable {}
}
