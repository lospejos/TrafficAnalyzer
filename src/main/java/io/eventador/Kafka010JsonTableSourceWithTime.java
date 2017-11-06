package io.eventador;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Implement proctime in JsonTableSource, based on Flink examples
 */
public class Kafka010JsonTableSourceWithTime extends Kafka010JsonTableSource implements DefinedProctimeAttribute {
    private String proctime = "proctime";

    public Kafka010JsonTableSourceWithTime(String topic, Properties properties, TypeInformation<Row> typeInfo, String proctime ) {
        super(topic, properties, typeInfo);
        this.proctime = proctime;
    }

    public Kafka010JsonTableSourceWithTime(String topic, Properties properties, TypeInformation<Row> typeInfo) {
        super(topic, properties, typeInfo);
    }

    @Override
    public String getProctimeAttribute() {
        return proctime;
    }
}