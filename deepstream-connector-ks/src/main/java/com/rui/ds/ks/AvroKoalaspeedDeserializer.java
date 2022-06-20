package com.rui.ds.ks;

import com.fs.jdbc.ks.event.AvroRecordEvent;
import com.fs.jdbc.ks.values.OperationType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AvroKoalaspeedDeserializer implements DeserializationSchema<RowData> {
    private final DataType dataType;
    private final RowType rowType;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final DeserializationSchema<AvroRecordEvent> nestedSchema;
    private final AvroKoalaSpeedToRowDataConverters.RowDataConverter runtimeConverter;
    public AvroKoalaspeedDeserializer(
            DataType dataType,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo
    ) {
        this.dataType = dataType;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.rowType = (RowType)dataType.getLogicalType();
        this.nestedSchema = AvroDeserializationSchema.forSpecific(AvroRecordEvent.class);
        this.runtimeConverter = AvroKoalaSpeedToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        AvroRecordEvent event = AvroRecordEvent.fromByteBuffer(ByteBuffer.wrap(message));
        Map<CharSequence, CharSequence> columnValues = event.getColumnValues();
        GenericRecord data = convertToGenericRecord(columnValues);
        RowData after = (RowData)runtimeConverter.convert(data);

        columnValues = event.getBeforeUpdatedColumns();
        data = convertToGenericRecord(columnValues);
        RowData before = (RowData)runtimeConverter.convert(data);
        int operationType = parseOperationType(event.getOperationType());

        switch (operationType) {
            case OperationType.UPDATE_CODE:
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(before);
                out.collect(after);
                break;
            case OperationType.DELETE_CODE:
                after.setRowKind(RowKind.DELETE);
                out.collect(after);
                break;
            case OperationType.INSERT_CODE:
            default:
                after.setRowKind(RowKind.INSERT);
                out.collect(after);
                break;
        }
    }

    private GenericRecord convertToGenericRecord(Map<CharSequence, CharSequence> columnValues) {
        GenericRecord record = new KoalaSpeedGenericRecord();

        for(Map.Entry<CharSequence, CharSequence> entry : columnValues.entrySet()) {
            record.put(entry.getKey().toString(), entry.getValue().toString());
        }

        return record;
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }


    private static int parseOperationType(Object obj) {
        if (obj == null) return OperationType.INSERT_CODE;
        if (obj instanceof Integer) return (Integer)obj;
        try {
            return Integer.parseInt(String.valueOf(obj));
        } catch (Throwable t) {
            return OperationType.INSERT_CODE;
        }
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class KoalaSpeedGenericRecord implements GenericRecord {
        private final Map<String, Object> map = new HashMap<>();

        @Override
        public void put(String key, Object v) {
            map.put(key, v);
        }

        @Override
        public Object get(String key) {
            return map.get(key);
        }

        @Override
        public void put(int i, Object v) {
            throw new UnsupportedOperationException("not support indexed method.");
        }

        @Override
        public Object get(int i) {
            throw new UnsupportedOperationException("not support indexed method.");
        }

        @Override
        public Schema getSchema() {
            throw new UnsupportedOperationException("not support schema.");
        }
    }
}
