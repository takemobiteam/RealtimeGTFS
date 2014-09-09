package readvehicle;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;









import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.VehicleDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;
import java.util.TimerTask;
import java.util.Date;
//parses protocol buffer format real time transit information of vehicle position and trip update
//vehicle position contains information about the vehicles including location , tripID, routeID, etc
//trip update contains information about delays, cancellations, changed routes
//GTFSrealtime is a general protocol format adopted by many transit agencies
//the the general information that could be retrieved can be found in GtfsRealtime.java
//MBTA may only provide part of that information 
//so we need to call the hasXXX() function to check if the attribute XXX exist or not before using it
//an intro to realtime gtfs is at https://developers.google.com/transit/gtfs-realtime/
//the reference of all the fields that can be retrieved is at:
//https://developers.google.com/transit/gtfs-realtime/reference#TripDescriptor


public class PeriodicUpdate extends TimerTask {
	private static final String databaseURI = "jdbc:mysql://localhost/vecInfo?"
			+ "user=mobitest&password=mobitest";
	private static Connection connect = null;
	private static Statement statement = null;



	@Override  
	public void run() {  
		// The logic of task/job that is going to be executed.  
		parseVehiclePosition();      
	}  


	//parse the vehicle position information
	//incluing lon/lat, trip id, route id, current stop
	public static void parseVehiclePosition(){
		String vec_routeid;
		String vec_tripid;
		double vec_lon;
		double vec_lat;
		double query_time=System.currentTimeMillis()/1000;
		System.out.println("begin insertion");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(databaseURI);
			statement = connect.createStatement();	

			//the url from MBTA that provide realtime vehicle position info
			URL _vehiclePositionsUrl = new URL("http://developer.mbta.com/lib/gtrtfs/Vehicles.pb");
			FeedMessage feed = FeedMessage.parseFrom(_vehiclePositionsUrl.openStream());
			System.out.println(feed.getEntityCount());
			//each vehicle currently on the road is an entity
			for (FeedEntity entity : feed.getEntityList()) {
				if (!entity.hasVehicle()) {
					continue;

				}

				vec_routeid="-1";
				vec_tripid="-1";
				vec_lon=-999;
				vec_lat=-999;


				VehiclePosition vehicle = entity.getVehicle();

				// the position of the vehicle
				if (vehicle.hasPosition()) {
					Position position = vehicle.getPosition();
					vec_lat=position.getLatitude();
					vec_lon=position.getLongitude();
					//System.out.println(position.getLatitude());
					//System.out.println(position.getLongitude());
				}

				// trip ID and route ID
				if (vehicle.hasTrip()){
					TripDescriptor trip_ = vehicle.getTrip();
					if (trip_.hasTripId()){
						vec_tripid = trip_.getTripId();
						//System.out.println(trip_.getTripId());
					}
					else{
						//System.out.println("No trip ID");
					}
					if (trip_.hasRouteId()){
						vec_routeid = trip_.getRouteId();
						//System.out.println(vec_routeid);
					}
					else{
						//System.out.println("No route ID");
					}
				}

				//The stop sequence index of the current stop. 
				//The meaning of current_stop_sequence 
				//(i.e., the stop that it refers to) is determined by current_status. 
				//If current_status is missing IN_TRANSIT_TO is assumed
				//current_status can take value: INCOMING_AT, STOPPED_AT, or IN_TRANSIT_TO
				if (vehicle.hasCurrentStopSequence()){
					//System.out.println(vehicle.getCurrentStopSequence());
				}

				//Identifies the current stop. 
				//The value must be the same as in stops.txt in the corresponding GTFS feed. 
				if (vehicle.hasStopId()){
					//System.out.println(vehicle.getStopId());
				}

				String sql = "REPLACE INTO vecloc set tripid=" +
						"'"+vec_tripid+"'"+ ", routeid=" +
						"'"+vec_routeid+"'"+ ", lon=" +
						vec_lon + ", lat =" + vec_lat + ", time=" + query_time + ";";
				//System.out.println(sql);
				statement.executeUpdate(sql);

			}
			System.out.println("end insertion");
			String sqlDropTempTable = "drop table if exists vecloctemp;";
			String sqlCreateTempTable = "CREATE TABLE vecloctemp  ( select * from vecloc);";
			String sqlTruncateTable = "truncate table vecloc;";
			statement.executeUpdate(sqlDropTempTable);
			statement.executeUpdate(sqlCreateTempTable);
			statement.executeUpdate(sqlTruncateTable);
			System.out.println("end copy");

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	public static void cleanTable(){
		double currentTime=System.currentTimeMillis()/1000;
		double targetTime=currentTime-3600;
		String sql = "delete from vecloc where time<"+targetTime+";";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(databaseURI);
			statement = connect.createStatement();
			statement.executeUpdate(sql);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	//parse trip update info,
	public static void parseTripUpdate(){

		try {
			URL _tripUpdateUrl = new URL("http://developer.mbta.com/lib/gtrtfs/Passages.pb");
			FeedMessage feed = FeedMessage.parseFrom(_tripUpdateUrl.openStream());
			System.out.println(feed.getEntityCount());

			for (FeedEntity entity : feed.getEntityList()) {
				if (!entity.hasTripUpdate()) {
					continue;
				}
				//Introduction to trip update is at:
				//https://developers.google.com/transit/gtfs-realtime/trip-updates
				TripUpdate tripUpdate = entity.getTripUpdate();
				List<StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList();
				if (tripUpdate.hasTrip()){
					System.out.println("trip: "+tripUpdate.getTrip());
				}
				
				//for each stop in this sequence, its actual status compared to the schedule
				for (StopTimeUpdate stopTimeUpdate : stopTimeUpdates ){
					
					//the stop sequence of this stop
					if (stopTimeUpdate.hasStopSequence()){
						System.out.println("stop sequence: "+stopTimeUpdate.getStopSequence());
					}
					//stop
					if (stopTimeUpdate.hasStopId()){
						System.out.println("stop id: "+stopTimeUpdate.getStopId());
					}

					//how does the stop arrival time compare to the schedule
					//can get the same information for departure time
					if (stopTimeUpdate.hasArrival()){
						StopTimeEvent stopTimeEvent = stopTimeUpdate.getArrival();

						//Delay (in seconds) can be positive (meaning that the vehicle is late) 
						//or negative (meaning that the vehicle is ahead of schedule). 
						//Delay of 0 means that the vehicle is exactly on time. 
						if (stopTimeEvent.hasDelay()){
							System.out.println("delay: "+stopTimeEvent.getDelay());
						}

						//Event as absolute time. In POSIX time (i.e., number of seconds since January 1st 1970 00:00:00 UTC). 
						if (stopTimeEvent.hasTime()){
							System.out.println("time: "+stopTimeEvent.getTime());
						}

						//Uncertainty applies equally to both time and delay. 
						//The uncertainty roughly specifies the expected error in true delay 
						//(but note, we don't yet define its precise statistical meaning). 
						//It's possible for the uncertainty to be 0, 
						//for example for trains that are driven under computer timing control.
						//If uncertainty is omitted, it is interpreted as unknown.
						if (stopTimeEvent.hasUncertainty()){
							System.out.println("uncertainty: "+stopTimeEvent.getUncertainty());
						}
					}
					System.out.println();
				}
			}

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PeriodicUpdate task = new PeriodicUpdate();
		Timer timer = new Timer();  
		//parseTripUpdate();
		timer.scheduleAtFixedRate(task, 0, 60000); 
		//parseVehiclePosition();
		//cleanTable();

	}

}
