<?php

//include('../database.php');

include('lookup.php');

include('lookup_ideal.php');


function connectToDatabase() {
	$db = new PDO('mysql:host=172.21.2.20;dbname=tempmon;charset=utf8', 'root', 'D1g1Fr3shT3mp');
	$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	return $db;
}

//A message is the same as a packet.  It contains the device id, sensor id, time and ADC reading.

function handle_data($data) {

	//We can not just iterate over the Msgs assuming we have multiple entries.  We need to handle
	//Situations where there is only one message

	$startTime = microtime(true);

	if($data && property_exists($data, 'Document')) {

		$Document = $data->Document;

		if($Document && property_exists($Document, 'Msg')) {

			$Msg = $Document->Msg;

			$Number_Of_Messages = count($Msg);

			if($Number_Of_Messages > 0) {

				$db = connectToDatabase();

				if($Number_Of_Messages == 1) {
					process_message($db, $Msg);
				} else {
					foreach($Msg AS $M) {
						process_message($db, $M);
					}
				}

				$db = null;

			} else {
				#error_log("[FS2 ERROR] Data contains 0 Messages.");
			}
		} else {
			#error_log("[FS2 ERROR] Data does not contain Msg.");
		}
	} else {
		#error_log("[FS2 ERROR] Data does not contain Document.");
	}

	$stopTime = microtime(true);

	$time_elapsed_ms = $stopTime - $startTime;

	echo "Time to process: " . $time_elapsed_ms . "\n";
}

function process_message($db, $Msg) {

	$Msg_Is_Valid = validate_message($Msg);

	if($Msg_Is_Valid) {

		$DataPoint 							= $Msg->DataPoint;

		$Gateway_Device_ID 			= parse_datapoint_stream($DataPoint, 0);

		$Sensor_Input_GPIO_Name = parse_datapoint_stream($DataPoint, 1);

		//Note:  Mac Address is reported with brackets and an exclaimation point, [00:13:A2:00:41:52:A1:7A]!, we clean this when fetching sensor from the database (see clean_sensor_mac function)
		$Sensor_MAC							= parse_datapoint_stream($DataPoint, 2);

		$Sensor_Input_GPIO_Pin	= parse_datapoint_stream($DataPoint, 3);

		//Returns ADC value, should be between 0 - 1023
		$Sensor_Input_Reading		= (int) $DataPoint->data;

		//Sensor time is reported in milliseconds, convert to UNIX timestamp
		$Sensor_Reading_Time		= (int) $DataPoint->timestamp / 1000;

		//$DEBUG_STRING = "Gateway: " . $Gateway_Device_ID . ", Sensor: " . $Sensor_MAC . ', Val: ' . $Sensor_Input_Reading . ' @ ' . $Sensor_Reading_Time;

		$start = microtime(true);

		$Reader = get_reader_from_Gateway_Device_ID($db, $Gateway_Device_ID);

		$stop = microtime(true);

		$time_elapsed_ms = $stop - $start;

		#echo "Time to get Reader: " . $time_elapsed_ms . "\n";

		$start = microtime(true);

		$Sensor	= get_sensor_from_sensor_mac($db, $Sensor_MAC);

		$stop = microtime(true);

		$time_elapsed_ms = $stop - $start;

		#echo "Time to get Sensor: " . $time_elapsed_ms . "\n";
		
		#error_log("Handling sensor with MAC: " . $Sensor_MAC);

		//Ensure we have the proper data fields
		if($Sensor && $Reader && $Sensor_Input_GPIO_Pin == "AD1") {

			if($Sensor->lookup_table == null) {
				$temperature = lookup($Sensor_Input_Reading);
			} else {
				$temperature = ideal_lookup($Sensor_Input_Reading);
			}
			
			#error_log("Found Sensor [ " . $Sensor_MAC . "] [" . $temperature . "]");
			//Just get current UTC timestamp
			$date = new DateTime(null, new DateTimeZone('UTC'));

			//When the gateways have accurate time
			//$timestamp = gmdate("Y-m-d H:i:s", $Sensor_Reading_Time);
			$timestamp = $date->format("Y-m-d H:i:s");

			$start = microtime(true);

			insert_into_sample_table($db, $Reader, $Sensor, $Sensor_Input_Reading, $temperature, $timestamp);

			$stop = microtime(true);

			$time_elapsed_ms = $stop - $start;

			#echo "Time to Insert Sample: " . $time_elapsed_ms . "\n";

			$start = microtime(true);

			insert_into_daily_temp_table($db, $Sensor, $temperature, $timestamp);

			$stop = microtime(true);

			$time_elapsed_ms = $stop - $start;

			#echo "Time to Insert Daily Temp: " . $time_elapsed_ms . "\n";

			$start = microtime(true);

			update_target_table($db, $Sensor->target, $temperature, $timestamp);

			$stop = microtime(true);

			$time_elapsed_ms = $stop - $start;

			#echo "Time to Update Target: " . $time_elapsed_ms . "\n";

			$start = microtime(true);

			update_sensor_table($db, $Sensor, $timestamp);

			$stop = microtime(true);

			$time_elapsed_ms = $stop - $start;

			#echo "Time to Update Sensor: " . $time_elapsed_ms . "\n";

			$start = microtime(true);

			update_reader_table($db, $Reader, $timestamp);

			$stop = microtime(true);

			$time_elapsed_ms = $stop - $start;

			#echo "Time to Update Reader: " . $time_elapsed_ms . "\n";

		} else {

			//Do some error reporting

			if(!$Sensor) {
				#error_log("[FS2 ERROR] Unable to Find Sensor with MAC: " . $Sensor_MAC);
			}

			if(!$Reader) {
				#error_log("[FS2 ERROR] Unable to Find Reader with Device ID: " . $Gateway_Device_ID);
			}

			if($Sensor_Input_GPIO_Pin != "AD1") {
				#error_log("[FS2 WARNING] Unknown GPIO Value: " . $Sensor_Input_GPIO_Pin . " on " . $Sensor_MAC);
			}

		}

	} else {

		#error_log("[FS2 ERROR] Invalid Message.");

		#error_log(print_r($Msg, true));

	}

}

function parse_datapoint_stream($DataPoint, $index) {
	return explode('/', $DataPoint->streamId)[$index];
}

function insert_into_daily_temp_table($db, $Sensor, $temperature, $timestamp) {
	try {

		$gt = $db->prepare("INSERT INTO daily_temp_table (target, tempval, updated) VALUES (?, ?, ?)");

		$gt->execute(
		  array(
		  	$Sensor->target,
		  	$temperature,
		  	$timestamp
		  	)
		);

	} catch(Exception $e) {

	  error_log( print_r( $e->getMessage() , true ) );

	}
}

function insert_into_sample_table($db, $Reader, $Sensor, $Sensor_Input_Reading, $temperature, $timestamp) {

	try {

		$gt = $db->prepare("INSERT INTO sample (location, target, reader, sensor, temp, tempval, remotecreated) VALUES (?, ?, ?, ?, ?, ?, ?)");

		$gt->execute(
		  array(
		  	$Reader->location,
		  	$Sensor->target,
		  	$Reader->id,
		  	$Sensor->id,
		  	$Sensor_Input_Reading,
		  	$temperature,
		  	$timestamp
		  	)
		);

	} catch(Exception $e) {

	  error_log( print_r( $e->getMessage() , true ) );

	}

}

function validate_message($Msg) {

  $valid = false;

  if($Msg && property_exists($Msg,'DataPoint')) {

    $DataPoint = $Msg->DataPoint;

    if(property_exists($DataPoint, 'timestamp') && property_exists($DataPoint, 'data') && property_exists($DataPoint, 'streamId')) {

    	$StreamId 		= $DataPoint->streamId;

    	$StreamIdSize = count(explode('/', $StreamId));

    	if($StreamIdSize == 4) {

    		$valid = true;

    	} else {
    		error_log("[FS2 ERROR] SteamId is an invalid length [ Length: " .$StreamIdSize. ", expeceting 4].");
    	}

    } else {
    	error_log("[FS2 ERROR] Datapoint missing critical information.");
    }

  } else {
  	error_log("[FS2 ERROR] Empty Msg and/or missing DataPoint.");
  }

  return $valid;

}

function clean_sensor_mac($Sensor_MAC) {

	return strtolower(str_replace(array('[',']','!'), array('','',''), $Sensor_MAC));

}

function update_target_table($db, $Target, $Temperature, $TimeStamp) {

	try {

		$r = $db->prepare("UPDATE target SET tempval = ?, lastSample = ? WHERE id = ?");

		$r->execute(array($Temperature, $TimeStamp, $Target));

	} catch(Exception $e) {

	  error_log( print_r( $e->getMessage() , true ) );

	}

}

function update_sensor_table($db, $Sensor, $TimeStamp) {
	try {

		$r = $db->prepare("UPDATE sensor SET lastlogin = ? WHERE id = ?");

		$r->execute(array($TimeStamp, $Sensor->id));

	} catch(Exception $e) {

	  error_log( print_r( $e->getMessage() , true ) );

	}
}

function update_reader_table($db, $Reader, $TimeStamp) {

	try {

		$r = $db->prepare("UPDATE reader SET lastlogin = ? WHERE id = ?");

		$r->execute(array($TimeStamp, $Reader->id));

	} catch(Exception $e) {

	  error_log( print_r( $e->getMessage() , true ) );

	}

}

function get_reader_from_Gateway_Device_ID($db, $Gateway_Device_ID) {

	$r = $db->prepare("SELECT * FROM reader WHERE deviceid = ?");

	$r->execute(array($Gateway_Device_ID));

	return $r->fetchObject();

}

function get_sensor_from_sensor_mac($db, $Sensor_MAC) {

	$r = $db->prepare("SELECT * FROM sensor WHERE sensormac = ?");

	$r->execute(array(clean_sensor_mac($Sensor_MAC)));

	return $r->fetchObject();
}


function process_logs() {
	if ($handle = opendir('logs/')) {

	    while (false !== ($entry = readdir($handle))) {

	        if ($entry != "." && $entry != "..") {

	        	$file = 'logs/' . $entry;

	        	echo "Processing: $entry\n";

	        	handle_data(json_decode(file_get_contents($file, true)));

	        	unlink($file);
	 
	        }
	    }

	    closedir($handle);
	}
}

process_logs();




?>
