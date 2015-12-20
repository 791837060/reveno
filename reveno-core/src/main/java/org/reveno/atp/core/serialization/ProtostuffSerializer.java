/**
 *  Copyright (c) 2015 The original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.reveno.atp.core.serialization;

import io.protostuff.Input;
import io.protostuff.LowCopyProtostuffOutput;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.reveno.atp.api.domain.RepositoryData;
import org.reveno.atp.api.exceptions.BufferOutOfBoundsException;
import org.reveno.atp.commons.ByteArrayObjectMap;
import org.reveno.atp.core.api.TransactionCommitInfo;
import org.reveno.atp.core.api.TransactionCommitInfo.Builder;
import org.reveno.atp.core.api.channel.Buffer;
import org.reveno.atp.core.api.serialization.RepositoryDataSerializer;
import org.reveno.atp.core.api.serialization.TransactionInfoSerializer;
import org.reveno.atp.core.serialization.protostuff.ZeroCopyBufferInput;
import org.reveno.atp.core.serialization.protostuff.ZeroCopyLinkBuffer;
import org.reveno.atp.utils.BinaryUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.reveno.atp.utils.BinaryUtils.sha1;

public class ProtostuffSerializer implements RepositoryDataSerializer, TransactionInfoSerializer {
	protected static final int SHA1_DIGEST_SIZE = 20;

	@Override
	public int getSerializerType() {
		return PROTO_TYPE;
	}
	
	@Override
	public boolean isRegistered(Class<?> type) {
		return names.containsKey(type);
	}

	@Override
	public void registerTransactionType(Class<?> txDataType) {
		byte[] key = sha1(txDataType.getName());
		registered.put(key, new ProtoTransactionTypeHolder(txDataType, RuntimeSchema.getSchema(txDataType)));
		names.put(txDataType, key);
	}

	@Override
	public void serialize(TransactionCommitInfo info, Buffer buffer) {
		buffer.writeLong(info.transactionId());
		buffer.writeLong(info.time());
		BinaryUtils.writeNullable(info.flag(), buffer);
		BinaryUtils.writeNullable(info.tag(), buffer);
		buffer.writeInt(info.transactionCommits().size());

		serializeObjects(buffer, info.transactionCommits());
	}

	@Override
	public TransactionCommitInfo deserialize(Builder builder, Buffer buffer) {
		changeClassLoaderIfRequired();

		long transactionId = buffer.readLong();
		long time = buffer.readLong();
		long flag = BinaryUtils.readNullable(buffer);
		long tag = BinaryUtils.readNullable(buffer);
		if (transactionId == 0 && time == 0) {
			throw new BufferOutOfBoundsException();
		}
		List<Object> commits = deserializeObjects(buffer);

		return builder.create().transactionId(transactionId)
				.time(time).transactionCommits(commits)
				.flag(flag).tag(tag);
	}

	@Override
	public void serialize(RepositoryData repository, Buffer buffer) {
		changeClassLoaderIfRequired();

        ZeroCopyLinkBuffer zeroCopyLinkBuffer = linkedBuff.get();
        LowCopyProtostuffOutput lowCopyProtostuffOutput = output.get();

        zeroCopyLinkBuffer.withBuffer(buffer);
        lowCopyProtostuffOutput.buffer = zeroCopyLinkBuffer;

        try {
            repoSchema.writeTo(lowCopyProtostuffOutput, repository);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
	}

	@Override
	public RepositoryData deserialize(Buffer buffer) {
		changeClassLoaderIfRequired();

		// TODO unnecessary allocation
        Input input = new ZeroCopyBufferInput(buffer, true);
        RepositoryData repoData = repoSchema.newMessage();
        try {
            repoSchema.mergeFrom(input, repoData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		return repoData;
	}

	@Override
	public void serializeCommands(List<Object> commands, Buffer buffer) {
		changeClassLoaderIfRequired();

		buffer.writeInt(commands.size());
		serializeObjects(buffer, commands);
	}

	@Override
	public List<Object> deserializeCommands(Buffer buffer) {
		changeClassLoaderIfRequired();

		return deserializeObjects(buffer);
	}


	public ProtostuffSerializer() {
		this(Thread.currentThread().getContextClassLoader());
	}

	public ProtostuffSerializer(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	protected void serializeObjects(Buffer buffer, List<Object> objs) {
		for (int i = 0; i < objs.size(); i++) {
			serializeObject(buffer, objs.get(i));
		}
	}

	@SuppressWarnings("unchecked")
	public void serializeObject(Buffer buffer, Object tc) {
        ZeroCopyLinkBuffer zeroCopyLinkBuffer = linkedBuff.get();
        LowCopyProtostuffOutput lowCopyProtostuffOutput = output.get();
		byte[] key = names.get(tc.getClass());
        Schema<Object> schema = (Schema<Object>) registered.get(key).schema;

        zeroCopyLinkBuffer.withBuffer(buffer);
        lowCopyProtostuffOutput.buffer = zeroCopyLinkBuffer;

        buffer.writeBytes(key, 0, SHA1_DIGEST_SIZE);
        buffer.markSize();
        try {
            schema.writeTo(lowCopyProtostuffOutput, tc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		buffer.writeSize();
    }
	
	protected List<Object> deserializeObjects(Buffer buffer) {
		int len = buffer.readInt();
		List<Object> commits =  new ArrayList<>(len);

		for (int i = 0; i < len; i++) {
			commits.add(i, deserializeObject(buffer));
		}
		return commits;
	}

	@SuppressWarnings("unchecked")
	public Object deserializeObject(Buffer buffer) {
		Input input = new ZeroCopyBufferInput(buffer, true);
		Schema<Object> schema = (Schema<Object>)registered.get(buffer, SHA1_DIGEST_SIZE).schema;
		int size = buffer.readInt();
		Object message = schema.newMessage();
		try {
		    buffer.limitNext(size);
		    schema.mergeFrom(input, message);
		    buffer.resetNextLimit();
		} catch (Exception e) {
		    throw new RuntimeException(e);
		}
		return message;
	}

	protected void changeClassLoaderIfRequired() {
		if (Thread.currentThread().getContextClassLoader() != classLoader) {
			Thread.currentThread().setContextClassLoader(classLoader);
		}
	}


	protected ThreadLocal<ZeroCopyLinkBuffer> linkedBuff = new ThreadLocal<ZeroCopyLinkBuffer>() {
		protected ZeroCopyLinkBuffer initialValue() {
			return new ZeroCopyLinkBuffer();
		}
	};
    protected ThreadLocal<LowCopyProtostuffOutput> output = new ThreadLocal<LowCopyProtostuffOutput>() {
        protected LowCopyProtostuffOutput initialValue() {
            return new LowCopyProtostuffOutput();
        }
    };
	protected ClassLoader classLoader;
	protected ByteArrayObjectMap<ProtoTransactionTypeHolder> registered = new ByteArrayObjectMap<>();
	protected Map<Class<?>, byte[]> names = new HashMap<>(64);
	protected final Schema<RepositoryData> repoSchema = RuntimeSchema.createFrom(RepositoryData.class);
	protected static final int PROTO_TYPE = 0x222;


	protected static class ProtoTransactionTypeHolder {
		public final Class<?> transactionType;
		public final Schema<?> schema;

		public ProtoTransactionTypeHolder(Class<?> transactionType, Schema<?> schema) {
			this.transactionType = transactionType;
			this.schema = schema;
		}
	}

}
