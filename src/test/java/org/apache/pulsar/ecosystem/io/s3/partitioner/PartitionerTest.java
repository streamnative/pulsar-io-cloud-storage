package org.apache.pulsar.ecosystem.io.s3.partitioner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.base.Supplier;
import junit.framework.TestCase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.List;

@RunWith(Parameterized.class)
public class PartitionerTest extends TestCase {

    @Parameterized.Parameter(0)
    public Partitioner<Object> partitioner;

    @Parameterized.Parameter(1)
    public String expected;

    @Parameterized.Parameter(2)
    public String expectedPartitionedPath;

    public static PulsarRecord<Object> pulsarRecord;

    public static String topic;

    @Parameterized.Parameters
    public static Object[][] data() {
        BlobStoreAbstractConfig blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig.setTimePartitionDuration("1d");
        blobStoreAbstractConfig.setTimePartitionPattern("yyyy-MM-dd");
        SimplePartitioner<Object> simplePartitioner = new SimplePartitioner<>();
        simplePartitioner.configure(blobStoreAbstractConfig);
        TimePartitioner<Object> dayPartitioner = new TimePartitioner<>();
        dayPartitioner.configure(blobStoreAbstractConfig);

        BlobStoreAbstractConfig hourConfig = new BlobStoreAbstractConfig();
        hourConfig.setTimePartitionDuration("4h");
        hourConfig.setTimePartitionPattern("yyyy-MM-dd-HH");
        TimePartitioner<Object> hourPartitioner = new TimePartitioner<>();
        hourPartitioner.configure(hourConfig);
        return new Object[][]{
                new Object[]{simplePartitioner, "partition-1" + Partitioner.PATH_SEPARATOR + "3221225506", "public/default/test/partition-1" + Partitioner.PATH_SEPARATOR + "3221225506"},
                new Object[]{dayPartitioner, "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506", "public/default/test/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506"},
                new Object[]{hourPartitioner, "2020-09-08-12" + Partitioner.PATH_SEPARATOR + "3221225506", "public/default/test/2020-09-08-12" + Partitioner.PATH_SEPARATOR + "3221225506"},
        };
    }

    @BeforeClass
    public static void startup(){
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(new MessageIdImpl(12,34,1));
        topic = TopicName.get("test").toString();
        pulsarRecord = PulsarRecord.<Object>builder()
                .message(mock)
                .topicName(topic)
                .partition(1)
                .build();
    }

    @Test
    public void testEncodePartition() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expected, encodePartition);
    }

    @Test
    public void testGeneratePartitionedPath() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        String partitionedPath = partitioner.generatePartitionedPath(topic, encodePartition);

        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expectedPartitionedPath, partitionedPath);
    }
}