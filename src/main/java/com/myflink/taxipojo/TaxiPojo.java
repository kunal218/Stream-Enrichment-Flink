package com.myflink.taxipojo;

public class TaxiPojo {

	private String driverName ;
	private String pickUp;
	private long pickUpTimestamp;
	private String drop;
	private long dropTimestamp;
	private long finalTimestamp;
	private int fare;
	private String pickupDate;
	private String dropDate;
	private String finalDate;
	
	public TaxiPojo(String driverName, String pickUp, long pickUpTimestamp, String drop, long dropTimestamp,
			long finalTimestamp,int fare) {
		super();
		this.driverName = driverName;
		this.pickUp = pickUp;
		this.pickUpTimestamp = pickUpTimestamp;
		this.drop = drop;
		this.dropTimestamp = dropTimestamp;
		this.finalTimestamp = finalTimestamp;
		this.fare=fare;
	}

	public TaxiPojo(String driverName, String pickUp,String pickupDate, String drop,
			String dropDate,String finalDate,int fare
			) {
	
		this.driverName = driverName;
		this.pickUp = pickUp;
		this.drop = drop;
		this.fare = fare;
		this.pickupDate = pickupDate;
		this.dropDate = dropDate;
		this.finalDate = finalDate;
	}

	public String getDriverName() {
		return driverName;
	}

	public String getPickUp() {
		return pickUp;
	}

	public long getPickUpTimestamp() {
		return pickUpTimestamp;
	}

	public String getDrop() {
		return drop;
	}

	public long getDropTimestamp() {
		return dropTimestamp;
	}

	public long getFinalTimestamp() {
		return finalTimestamp;
	}
	public int getFare() {
		return fare;
	}
	@Override
	public String toString() {
		return driverName+","+pickUp+","+pickUpTimestamp+","+drop+","+dropTimestamp+
				","+finalTimestamp+","+fare+","+pickupDate+","+dropDate+","+finalDate;
	}

	public String getPickupDate() {
		return pickupDate;
	}

	public String getDropDate() {
		return dropDate;
	}

	public String getFinalDate() {
		return finalDate;
	}
	

	

	
	
	
}
