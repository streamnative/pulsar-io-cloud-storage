/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.junit.Test;

public class ParquetMultiRecordUnionTest {

    @Test
    public void testSingleRecordUnionAllowsGeneratedNamespace() {
        org.apache.avro.Schema nativeSchema = SchemaBuilder.record("ValueRecord")
                .fields().requiredString("content").endRecord();
        GenericData.Record nativeRecord = new GenericData.Record(nativeSchema);
        nativeRecord.put("content", "value");
        GenericRecord pulsarRecord = new GenericAvroRecord(
                null, nativeSchema, toPulsarFields(nativeSchema), nativeRecord);

        org.apache.avro.Schema outputSchema = SchemaBuilder.record("ValueRecord")
                .namespace("_value")
                .fields().requiredString("content").endRecord();
        org.apache.avro.Schema nullableOutputSchema = org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), outputSchema);

        org.apache.avro.generic.GenericRecord converted =
                AvroRecordUtil.convertGenericRecord(pulsarRecord, nullableOutputSchema);

        assertEquals("_value.ValueRecord", converted.getSchema().getFullName());
        assertEquals("value", converted.get("content").toString());
    }

    @Test
    public void testWriteNonFirstRecordUnionBranch() throws Exception {
        org.apache.avro.Schema rootSchema = createSchema();
        org.apache.avro.Schema secondReportSchema = rootSchema.getField("report").schema().getTypes().get(1);
        org.apache.avro.Schema secondSnapshotSchema = secondReportSchema.getField("snapshots")
                .schema().getElementType();

        GenericData.Record snapshot = new GenericData.Record(secondSnapshotSchema);
        snapshot.put("id", 1L);
        snapshot.put("state", 42L);
        GenericData.Record report = new GenericData.Record(secondReportSchema);
        report.put("snapshots", List.of(snapshot));
        GenericData.Record envelope = new GenericData.Record(rootSchema);
        envelope.put("report", report);

        GenericRecord pulsarRecord = new GenericAvroRecord(
                null, rootSchema, toPulsarFields(rootSchema), envelope);
        Record<GenericRecord> record = mock(Record.class);
        when(record.getValue()).thenReturn(pulsarRecord);

        ParquetFormat format = new ParquetFormat();
        format.initSchema(mockPulsarSchema(rootSchema));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(format.recordWriterBuf(List.of(record).iterator()).array());

        ParquetInputFile inputFile = new ParquetInputFile("multi-record-union.parquet", stream);
        try (ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                .<org.apache.avro.generic.GenericRecord>builder(inputFile)
                .withDataModel(GenericData.get())
                .build()) {
            org.apache.avro.generic.GenericRecord writtenEnvelope = reader.read();
            assertNotNull(writtenEnvelope);
            org.apache.avro.generic.GenericRecord writtenReport =
                    (org.apache.avro.generic.GenericRecord) writtenEnvelope.get("report");
            assertEquals("SecondReport", writtenReport.getSchema().getName());
            List<?> writtenSnapshots = (List<?>) writtenReport.get("snapshots");
            org.apache.avro.generic.GenericRecord writtenSnapshot =
                    (org.apache.avro.generic.GenericRecord) writtenSnapshots.get(0);
            assertEquals(42L, writtenSnapshot.get("state"));
        }
    }

    private static Schema<GenericRecord> mockPulsarSchema(org.apache.avro.Schema avroSchema) {
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .name(avroSchema.getName())
                .type(SchemaType.AVRO)
                .schema(avroSchema.toString().getBytes(StandardCharsets.UTF_8))
                .properties(Collections.emptyMap())
                .build();
        Schema<GenericRecord> schema = mock(Schema.class);
        when(schema.getSchemaInfo()).thenReturn(schemaInfo);
        return schema;
    }

    private static List<Field> toPulsarFields(org.apache.avro.Schema schema) {
        return schema.getFields().stream()
                .map(field -> new Field(field.name(), field.pos()))
                .collect(Collectors.toList());
    }

    private static org.apache.avro.Schema createSchema() {
        org.apache.avro.Schema firstState = SchemaBuilder.record("FirstState")
                .fields().requiredLong("value").endRecord();
        org.apache.avro.Schema firstSnapshot = SchemaBuilder.record("FirstSnapshot")
                .fields()
                .requiredLong("id")
                .name("state").type(firstState).noDefault()
                .endRecord();
        org.apache.avro.Schema firstReport = SchemaBuilder.record("FirstReport")
                .fields()
                .name("snapshots").type(org.apache.avro.Schema.createArray(firstSnapshot)).noDefault()
                .endRecord();
        org.apache.avro.Schema secondSnapshot = SchemaBuilder.record("SecondSnapshot")
                .fields().requiredLong("id").requiredLong("state").endRecord();
        org.apache.avro.Schema secondReport = SchemaBuilder.record("SecondReport")
                .fields()
                .name("snapshots").type(org.apache.avro.Schema.createArray(secondSnapshot)).noDefault()
                .endRecord();
        return SchemaBuilder.record("Envelope")
                .fields()
                .name("report").type(org.apache.avro.Schema.createUnion(firstReport, secondReport)).noDefault()
                .endRecord();
    }
}
