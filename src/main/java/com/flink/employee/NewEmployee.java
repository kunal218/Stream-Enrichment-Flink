package com.flink.taxi;

import java.awt.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.myflink.taxipojo.TaxiPojo;

import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class NewEmployee {
	
	public static class EnrichedEmployee extends RichCoFlatMapFunction<Tuple3<Integer,String,Integer>, Tuple2<Integer,String>, Tuple3<Integer, String, String>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 4605540049295177491L;

		int counter=0;
		
	private	MapState<Integer, String> state;
	
		@Override
		public void open(Configuration parameters) throws Exception {
			// TODO Auto-generated method stub
		
			MapStateDescriptor descriptor =
					new MapStateDescriptor<Integer, String>("empdesc", TypeInformation.of(new TypeHint<Integer>() {
					}), TypeInformation.of(new TypeHint<String>() {
					}));
			state=getRuntimeContext().getMapState(descriptor);
		}

		public void flatMap1(Tuple3<Integer, String, Integer> value, Collector<Tuple3<Integer, String, String>> out)
				throws Exception {
			if(value.f0 != null && value.f1!=null) {
			//System.out.println("flatmap1"+value.f2);
				//System.out.println("putting in state key->"+value.f2);
			state.put(value.f2, new String(value.f0+","+value.f1));
			}
			
		}

		public void flatMap2(Tuple2<Integer, String> value, Collector<Tuple3<Integer, String, String>> out)
				throws Exception {
			if(value.f0!=null && value.f1!=null) {
			Integer key = value.f0;
			if((state.get(key))!=null) {
			String record = state.get(key);
			//System.out.println("record"+record);
			String [] data=record.split(",");
			Tuple3<Integer, String, String> output=new Tuple3<Integer, String, String>(Integer.parseInt(data[0]), data[1], value.f1);
			state.remove(key);
			out.collect(output);
			}
			}
		}
		

		
		
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		 env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "consumer-new");
		// properties.setProperty("auto.offset.reset","earliest");
		properties.setProperty("auto.offset.reset", "latest");

		// properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,System.currentTimeMillis()+"");
		DataStream<String> employeeStream = env
				.addSource(new FlinkKafkaConsumer<String>("emp", new SimpleStringSchema(), properties));

		
		DataStream<String> deptStream = env
				.addSource(new FlinkKafkaConsumer<String>("dept", new SimpleStringSchema(), properties));
		
		KeyedStream<Tuple3<Integer,String,Integer>,Tuple> mapEmployee = employeeStream.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -4187003150185491541L;

			public Tuple3<Integer, String, Integer> map(String value) throws Exception {
				String [] data=value.split("\\,");
				return new Tuple3<Integer, String, Integer>(Integer.parseInt(data[0]), data[1], Integer.parseInt(data[2]));
			}
		})
				.keyBy(2);
		
		KeyedStream<Tuple2<Integer, String>, Tuple> mapDept = deptStream.map(new MapFunction<String, Tuple2<Integer, String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 6600257357200057531L;

			public Tuple2<Integer, String> map(String value) throws Exception {
				// TODO Auto-generated method stub
				String data[]=value.split(",");
				return new Tuple2<Integer, String>(Integer.parseInt(data[0]),data[1]);
			}
		})
				.keyBy(0);
		
		
		mapEmployee.connect(mapDept)
		.flatMap(new EnrichedEmployee())
		.print();
		
		
		
	
		
		
		env.execute();
	}

}