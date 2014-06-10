package main.scala

import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage

/**
 *
 * MQTT publisher
 * @author Prabeesh K
 * @mail prabsmails@gmail.com
 *
 */

object Publisher {

  def main(args: Array[String]) {

    val brokerUrl = "tcp://10.30.9.105:1883"
    val topic = "hello"


    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")

    // mqtt client with specific url and client id
    val client: MqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)
    client connect()

    val msgTopic = client.getTopic(topic)

    val msg = "Mqtt publisher test data"

    while (true) {

      val message = new MqttMessage(msg.getBytes)
      msgTopic.publish(message)
      println("Published data. Topic: " + msgTopic.getName + " Message: " + msg)
      Thread.sleep(100)

   }
    client.disconnect()

  }
}
