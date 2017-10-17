package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FlinkReadWriteKafkaSinker {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 4) {
                System.out.println("\nUsage: FlinkReadKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
                return;
            }

            // setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            // specify JSON field names and types
            TypeInformation<Row> typeInfo = Types.ROW(
                    new String[] { "flight", "timestamp_verbose", "msg_type", "track", "timestamp", "altitude", "counter", "lon", "icao", "vr", "lat", "speed" },
                    new TypeInformation<?>[] { Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() }
            );

            // create a new tablesource of JSON from kafka
            KafkaJsonTableSource kafkaTableSource = new Kafka010JsonTableSourceWithTime(
                    params.getRequired("read-topic"),
                    params.getProperties(),
                    typeInfo,
                    "proctime");

            // run some SQL to filter results where a key is not null
            String sql = "SELECT icao, count(icao) AS pings "
                          + "FROM flights "
                          + "WHERE icao IS NOT null "
                          + "GROUP BY TUMBLE(proctime, INTERVAL '5' SECOND), icao";

            // register the table and apply sql to stream
            tableEnv.registerTableSource("flights", kafkaTableSource);
            Table flight_table = tableEnv.sql(sql);

            // stdout debug stream, prints raw datastream to logs
            DataStream<Row> planeRow = tableEnv.toAppendStream(flight_table, Row.class);
            planeRow.print();

            // stream for feeding tumbling window count back to Kafka
            TupleTypeInfo<Tuple2<String, Long>> planeTupleType = new TupleTypeInfo<>(Types.STRING(), Types.LONG());
            DataStream<Tuple2<String, Long>> planeTuple = tableEnv.toAppendStream(flight_table, planeTupleType);

            FlinkFixedPartitioner partition =  new FlinkFixedPartitioner();

            // create new tablesink of JSON to kafka
            KafkaJsonTableSink kafkaTableSink = new Kafka09JsonTableSink(
                    params.getRequired("write-topic"),
                    params.getProperties(),
                    partition);

            flight_table.writeToSink(kafkaTableSink);

            env.execute("FlinkReadWriteKafkaJSON");
        }

}
