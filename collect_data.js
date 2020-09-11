/*
 * Author: Daniel Holmlund <daniel.w.holmlund@Intel.com>
 * Copyright (c) 2015 Intel Corporation.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

'use strict';
// A library to colorize console output
var chalk = require('chalk');

// Require MQTT and setup the connection to the broker
var mqtt = require('mqtt');

const Influx = require('influx');
const Cron = require('node-cron');

// Libary to parse command line arguments
const commandLineArgs = require('command-line-args')
const getUsage = require('command-line-usage');

// Command line argument definitions
const cmdLineArgsDefinitions = require('./optionDefinitions')
const optionDefinitions = cmdLineArgsDefinitions.optionDefinitions
const sections = cmdLineArgsDefinitions.sections

// Parse the command line arguments
const options = commandLineArgs(optionDefinitions);
// Print the help text
if (options.main.help) {
    info(getUsage(sections))
    process.exit();
}

// Set the default port number
var port = 1883;
if (options.encryption.tls != undefined) {
  port = 8883;
}
if (options.connection.port != undefined){
  port = options.connection.port;
}

  // Log virtual-sensor as started
  info("Connecting to MQTT broker at mqtt://" + options.connection.hostname + ":" + port + "/");

  // Create an MQTT client
  var client = mqtt.connect("mqtt://" + options.connection.hostname + ":" + port + "/");


const INFLUX_DB_HOST = options.connection.dbhost;

const sensorsimulDb = new Influx.InfluxDB({
  host: INFLUX_DB_HOST,
  database: 'sensor_simulator',
  schema: [
    {
      measurement: 'sensor',
      fields: {
        sensor_id: Influx.FieldType.STRING,
        value: Influx.FieldType.FLOAT,
        timestamp: Influx.FieldType.STRING,
      },
      tags: [
        'sensor'
      ]
    },
  ],
});


setupDb()


// Functions

function setupDb() {
  return new Promise((resolve, reject) => {
    sensorsimulDb.getDatabaseNames().then(names => {
      if (!names.find(n => n === 'sensor_simulator')) {
        sensorsimulDb.createDatabase('sensor_simulator').then(resolve).catch(reject);
      }collectSensorData();
      resolve();
    }).catch(reject);
  });
}

function collectSensorData() {

    var topic = "sensors/" + options.sensor.name + "/data";
    console.log("subscribing to topics");
    client.subscribe(topic,{qos:1}); //single topic

}

function DbWrite(json){
  console.log("write message on db");
  let points = [];
      points.push({
        measurement: 'sensor',
        fields: {
          sensor_id: json.sensor_id,
          value: json.value,
          timestamp: json.timestamp,

        },
        tags: { sensor: json.sensor_id },
      });
      sensorsimulDb.writePoints(points)
}

//handle incoming messages
client.on('message',function(topic, message, packet){
  console.log(message);
  var json = JSON.parse(message);

  DbWrite(json)
});

client.on("connect",function(){ 
info("connected  "+ client.connected);
});

//handle errors
client.on("error",function(error){
info("Can't connect" + error);
process.exit(1)
});

function info(str) {
    // Log virtual-sensor as started
    if (options.main.silent != true) {
        console.log(chalk.bold.yellow(str));
    }
}