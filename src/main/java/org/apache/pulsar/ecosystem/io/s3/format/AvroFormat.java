package org.apache.pulsar.ecosystem.io.s3.format;

import com.google.common.io.ByteSource;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.functions.api.Record;

public class AvroFormat<C extends BlobStoreAbstractConfig, T> implements Format<C, Record<T>> {

    @Override
    public String getExtension() {
        return ".avro";
    }

    @Override
    public ByteSource recordWriter(C config, Record<T> record) {
//        record.getSchema().getSchemaInfo().getType()
//        record.getValue()
        byte[] data = record.getMessage().get().getData();
        GenericRecord recordValue = (GenericRecord)record.getValue();
        switch (record.getSchema().getSchemaInfo().getType()){
            case AVRO:
                return ByteSource.wrap(record.getSchema().encode((T)recordValue));
            default:
                Schema schema = ReflectData.get().getSchema(recordValue.getClass());
                AvroWriter<GenericRecord> avroWriter = new AvroWriter<>(schema, true);
                avroWriter.write(recordValue);
                return ByteSource.wrap(avroWriter.write(recordValue));
        }
    }
}
