# MQTT Taxi Dispatch System

Java distributed system simulation for taxi ride scheduling using the MQTT protocol.

## Overview

This project simulates a taxi dispatch service where customers request rides, dispatchers collect offers from available taxis, and the best offer is selected according to customer preferences.

The system uses MQTT topics for communication between all participants and supports concurrent execution using multiple threads.

## Features

* Customer ride requests
* Dispatcher-based ride management
* Multiple taxi vehicles running concurrently
* MQTT communication using Eclipse Paho
* JSON message exchange using Gson
* Asynchronous offer collection
* Customer confirmation (ACK) mechanism
* Dynamic taxi topic subscriptions based on location
* Ride completion logging

## Technologies

* Java
* MQTT (Eclipse Paho)
* Gson
* Maven
* Multithreading
* Concurrent Collections

## Architecture

Customer → Dispatcher → Taxi Fleet → Dispatcher → Customer

### Workflow

1. Customer creates a ride request.
2. Dispatcher receives the request.
3. Dispatcher publishes the request to taxi topics.
4. Available taxis send ride offers.
5. Dispatcher collects offers and selects the best one.
6. Customer confirms the selected offer.
7. Dispatcher notifies the winning taxi.
8. Ride is completed and logged.

## Project Structure

* Customer.java – customer simulation
* Dispatcher.java – ride coordination and offer selection
* Taxi.java – taxi vehicle simulation
* RideRequest.java – ride request model
* MqttHelper.java – MQTT communication helper

## Running the Project

1. Start an MQTT broker (Mosquitto or HiveMQ).
2. Run Dispatcher.
3. Run Taxi.
4. Run Customer.

## Author

Đina Matić
