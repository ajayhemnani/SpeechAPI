package com.google.cloud.dataflow.examples.smelab;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

/* Simple class representing metadata about a package's pickup or
 * drop-off. Is meant to be read from a log line. 
 */
@DefaultCoder(AvroCoder.class)
public class PackageActivityInfo {
	private static final Logger LOG = LoggerFactory.getLogger(PackageActivityInfo.class);
	
	private boolean isArrival; // If true, is an arrival event; otherwise departing.
	private String location; // Two letter code for factory id or "CU" for customer counter.
	private Date time; // Time of the drop off or pickup activity.
	private int truckId; // ID of the truck doing the drop off or pick up. 0 if customer.
	private String packageId; // ID of the package in transit.

	public PackageActivityInfo() {}
	public PackageActivityInfo(boolean isArrival, String location, Date time, int truckId, String packageId) {
		this.isArrival = isArrival;
		this.location = location;
		this.time = time;
		this.truckId = truckId;
		this.packageId = packageId;
	}
	
	@Override
	public String toString() {
		return "PackageActivityInfo [isArrival=" + isArrival + ", location=" + location + ", packageId=" + packageId + "]";
	}

	public boolean isArrival() {
		return isArrival;
	}

	public String getLocation() {
		return location;
	}

	public Date getTime() {
		return time;
	}

	public int getTruckId() {
		return truckId;
	}

	public String getPackageId() {
		return packageId;
	}
	
	public void setArrival(boolean arrival) {
		isArrival = arrival;
	}
	public void setLocation(String loc) {
		location = loc;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public void setTruckId(int truckId) {
		this.truckId = truckId;
	}
	public void setPackageId(String packageId) {
		this.packageId = packageId;
	}

	// Return null if there is any error parsing.
	// Delimits line by the given delimeter.
	// Logline: "0, AN, 1467394122, 423, 372A3SZ4J98"
	public static PackageActivityInfo Parse(String logLine) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logLine.split(",");
			if (pieces.length != 5)
				return null;
			int isArrivalInt = Integer.parseInt(pieces[0].trim());
			if (isArrivalInt == 0) {
				pickup.isArrival = false;
			} else if (isArrivalInt == 1) {
				pickup.isArrival = true;
			} else {
				return null;
			}
			pickup.location = pieces[1].trim();
			pickup.time = new Date(Long.parseLong(pieces[2].trim()) * 1000);
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}
	
	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator("invalidLogLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			String logLine = c.element();
			PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
			if (info == null) {
				invalidLines.addValue(1L);
			} else {
				c.output(info);
			}
		}
	}
	
	public static final String[] MINI_LOG = {
      "0, AN, 1467394122, 423, 372A3SZ4J98",
      "0, AN, 1467394122, 423##############",
       "1, AK, 1467394123, 102, 320893JSEFE",
      "404 - broken message",
      "1, AN, 1467394123, 423, 372A3SZ4J98",
       "1, AK, 1467394120, 102, 320893JSEFE",
      "1, BK, 1467494150, 190, 3JISE23423J",
      "0, AK, 1467494123, 12, 320893JSEFE",
      "404 - broken message",
      "0, AN, 1467395123, 42, 372A3SZ4J98",
      "0, AK, 1467394150, 12, 320893JSEFE",
      "0, BK, 1467495150, 19, 3JISE23423J",
       "1, BK, 1467495150, 19, 3JISE234",
       "1, BK, 1467495150, 19, 34HWINF"
      };
}