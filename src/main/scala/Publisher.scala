package main.scala

import resource._

import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

/**
 *
 * MQTT publisher
 * @author Prabeesh K
 * @mail prabsmails@gmail.com
 *
 */

object Publisher {

  def main(args: Array[String]) {
    val brokerUrl = "tcp://localhost:1883"
    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")
    
    // mqtt client with specific url and client id
    val client = managed(new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)).map(x => {
    val topic = "foo"
    val msg = "Hello world test data"

    x.connect()

    val msgTopic = x.getTopic(topic)
    val message = new MqttMessage(msg.getBytes("utf-8"))

    while (true) {
      msgTopic.publish(message)
      println("Publishing Data, Topic : %s, Message : %s".format(msgTopic.getName, message))
      Thread.sleep(100)
    }
   }).opt
  
   }
}
