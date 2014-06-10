package main.scala

import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttClientPersistence
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttMessage

/**
 *
 * MQTT subcriber
 * @author Prabeesh K
 * @mail prabsmails@gmail.com
 *
 */

object Subscriber {

  var client: MqttClient = _

  def main(args: Array[String]) {

    val brokerUrl: String = "tcp://mqttbroker:1883"
    val topic: String = "hello"

    //Set up persistence for messages 
    val persistence: MqttClientPersistence = new MemoryPersistence()

    //Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    val client: MqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)

    //Connect to MqttBroker    
    client.connect()

    //Subscribe to Mqtt topic
    client.subscribe(topic)

    //Callback automatically triggers as and when new message arrives on specified topic
    val callback: MqttCallback = new MqttCallback() {

      //Handles Mqtt message 
      override def messageArrived(arg0: String, arg1: MqttMessage) {
        println(new String(arg1.getPayload()))
      }

      override def deliveryComplete(arg0: IMqttDeliveryToken) {
      }

      override def connectionLost(arg0: Throwable) {
        System.err.println("Connection lost " + arg0)

      }

    }

    //Set up callback for MqttClient
    client.setCallback(callback)

  }
}
