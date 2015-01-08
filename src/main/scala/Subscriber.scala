package main.scala

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

/**
 *
 * MQTT subcriber
 * @author Prabeesh K
 * @mail prabsmails@gmail.com
 *
 */

object Subscriber {

  def main(args: Array[String]) {

    val brokerUrl = "tcp://localhost:1883"
    val topic = "foo"

    //Set up persistence for messages 
    val persistence = new MemoryPersistence

    //Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    val client = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)

    //Connect to MqttBroker    
    client.connect

    //Subscribe to Mqtt topic
    client.subscribe(topic)

    //Callback automatically triggers as and when new message arrives on specified topic
    val callback = new MqttCallback {

      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        println("Receiving Data, Topic : %s, Message : %s".format(topic, message))
      }

      override def connectionLost(cause: Throwable): Unit = {
         println(cause)
       }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {

      }
    }

    //Set up callback for MqttClient
    client.setCallback(callback)

  }
}
