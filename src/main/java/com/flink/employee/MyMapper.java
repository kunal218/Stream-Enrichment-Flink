package com.flink.taxi;

import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.myflink.taxipojo.TaxiPojo;

public class MyMapper implements MapFunction<String, Tuple2<TaxiPojo, Integer>>{

	/**
	 * 
	 */
	public String getTime(long milis) {
		Calendar calendar=Calendar.getInstance();
		
		calendar.setTimeInMillis(milis);
		//System.out.println("\ncalender"+calendar);
			Timestamp timestamp=new Timestamp(calendar.getTimeInMillis());
			return timestamp.toString();
	}
	private static final long serialVersionUID = 2730387930453613688L;

	public Tuple2 map(String value) throws Exception {
		String[] data = value.split(",");
	//	System.out.println("In MApper :"+getTime(Long.parseLong(data[2])));
		return new Tuple2<TaxiPojo, Integer>(new TaxiPojo(data[0],data[1] , getTime(Long.parseLong(data[2])), data[3], getTime(Long.parseLong(data[4])),getTime(Long.parseLong(data[5])),Integer.parseInt(data[6]) ),Integer.parseInt(data[6]));
	}
}
