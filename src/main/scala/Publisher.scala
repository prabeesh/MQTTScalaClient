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

    val brokerUrl = "tcp://10.31.0.38:1883"
    val topic = "hello"
    val msg = "Hello world test data"
    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")

    try {
      // mqtt client with specific url and client id
      val client: MqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)
      client connect()

      val msgTopic = client.getTopic(topic)

      while (true) {

        val message = new MqttMessage(msg.getBytes("utf-8"))
        msgTopic.publish(message)
        println("Publishing Data, Topic : %s, Message : %s".format(msgTopic.getName, message))
        Thread.sleep(100)

      }
      client.disconnect()
    }

    catch {
      case e: MqttException => println("Exception Caught: " + e)
    }

  }
}
