package org.apache.pulsar.ecosystem.io.s3.format;

import com.google.common.io.ByteSource;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;

import java.io.IOException;
import java.util.Optional;

public class ParquetFormat<C,T> implements Format<C, Record<T>>{
    @Override
    public String getExtension() {
        return ".parquet";
    }

    @Override
    public ByteSource recordWriter(C config, Record<T> record) {

        record.getDestinationTopic();
        record.getTopicName();
        record.getEventTime();
        record.getPartitionId();
        record.getRecordSequence();
        record.getProperties();

        Optional<Message<T>> message = record.getMessage();
        SchemaInfo schemaInfo = record.getSchema().getSchemaInfo();

        Message<T> tMessage = message.get();
        T recordValue = record.getValue();
        Schema schema;
        switch (record.getSchema().getSchemaInfo().getType()){
            case AVRO:
                schema = parseAvroSchema(schemaInfo.getSchemaDefinition());
                break;
            default:
                schema = ReflectData.get().getSchema(recordValue.getClass());
                break;
        }

        ParquetWriter<Object> build = null;
        try {
            S3ParquetOutputFile file = new S3ParquetOutputFile();

            build = AvroParquetWriter
                    .builder(file)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withSchema(schema)
                    .build();
            build.write(recordValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeQuietly(build);
        return null;
    }

    protected static org.apache.avro.Schema parseAvroSchema(String schemaJson) {
        final Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schemaJson);
    }

    protected static Schema extractAvroSchema(SchemaDefinition schemaDefinition, Class pojo) {
        try {
            return parseAvroSchema(pojo.getDeclaredField("SCHEMA$").get(null).toString());
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException ignored) {
            return schemaDefinition.getAlwaysAllowNull() ? ReflectData.AllowNull.get().getSchema(pojo)
                    : ReflectData.get().getSchema(pojo);
        }
    }

    private static class S3ParquetOutputFile implements OutputFile {
        private static final int DEFAULT_BLOCK_SIZE = 0;


        S3ParquetOutputFile() {

        }

        @Override
        public PositionOutputStream create(long l) throws IOException {
            return null;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long l) throws IOException {
            return null;
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}
