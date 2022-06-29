package com.rui.ds.ks;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;

public class AvroKoalaSpeedToRowDataConverters {

    @FunctionalInterface
    public interface RowDataConverter extends Serializable {
        Object convert(Object object);
    }

    public static RowDataConverter createRowConverter(RowType rowType) {
        final AvroToRowDataConverters.AvroToRowDataConverter[] fieldConverters = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .map(AvroKoalaSpeedToRowDataConverters::createNullableConverter)
                .toArray(AvroToRowDataConverters.AvroToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();
        final List<String> fieldNames = rowType.getFieldNames();

        return data -> {
            GenericRecord genericRecord = (GenericRecord) data;
            GenericRowData row = new GenericRowData(arity);
            for(int i = 0; i < arity; ++i) {
                String fieldName = fieldNames.get(i);
                Object value = genericRecord.get(fieldName);
                row.setField(i, fieldConverters[i].convert(value));
            }

            return row;
        };
    }

    private static AvroToRowDataConverters.AvroToRowDataConverter createNullableConverter(LogicalType type) {
        final AvroToRowDataConverters.AvroToRowDataConverter converter = createConverter(type);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }

            return converter.convert(avroObject);
        };
    }

    private static AvroToRowDataConverters.AvroToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case BOOLEAN: // boolean
                return obj -> Boolean.parseBoolean(String.valueOf(obj));
            case TINYINT:
                //return avroObject -> ((Integer)Integer.parseInt(String.valueOf(avroObject))).byteValue();
            case SMALLINT:
                //return avroObject -> ((Integer)Integer.parseInt(String.valueOf(avroObject))).shortValue();
            case INTEGER: // int
                return obj -> Integer.parseInt(String.valueOf(obj));
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long

                return obj -> {
                    return Long.parseLong(String.valueOf(obj));
                };
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
                return obj -> Float.parseFloat(String.valueOf(obj));
            case DOUBLE: // double
                //return avroObject -> avroObject;
                return obj -> Double.parseDouble(String.valueOf(obj));
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case CHAR:
            case VARCHAR:

                return obj -> {
                    return StringData.fromString(String.valueOf(obj));
                };
            case DECIMAL:
                return obj -> String.valueOf(obj);
            case BINARY:
            case VARBINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
