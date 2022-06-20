package com.rui.ds.ks;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class AvroKoalaspeedFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    public AvroKoalaspeedFormat() {
        // noop;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context,
            DataType dataType
    ) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(dataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(dataType);

        // use logical types during runtime for parsing
        //final List<LogicalType> parsingTypes = dataType.getLogicalType().getChildren();

        // create runtime class
        return new AvroKoalaspeedDeserializer(dataType, converter, producedTypeInfo);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
