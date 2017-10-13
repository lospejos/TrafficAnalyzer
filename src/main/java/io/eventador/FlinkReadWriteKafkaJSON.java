package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkReadWriteKafkaJSON {
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
            KafkaJsonTableSource kafkaTableSource = new Kafka010JsonTableSource(
                    params.getRequired("read-topic"),
                    params.getProperties(),
                    typeInfo);

            // run some SQL to filter results where a key is not null
            String sql = "SELECT icao FROM flights WHERE icao is not null";
            tableEnv.registerTableSource("flights", kafkaTableSource);
            Table result = tableEnv.sql(sql);

            DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
            resultSet.print();

            env.execute("FlinkReadWriteKafkaJSON");
        }
}

