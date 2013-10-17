package main.scala

import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttClientPersistence
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttTopic

/**
 *
 * MQTT publisher
 * @author Prabeesh K
 * @mail prabsmails@gmail.com
 *
 */

object Publisher {

  var client: MqttClient = _
  def main(args: Array[String]) {

    val brokerUrl: String = "tcp://mqttbrokerUrl:1883"
    val topic: String = "hello"

    try {
      // Creating new persistence for mqtt client
      var peristance: MqttClientPersistence = new MqttDefaultFilePersistence("/tmp")

      // mqtt client with specific url and client id
      client = new MqttClient(brokerUrl, MqttClient.generateClientId(), peristance)

      client.connect()

    } catch {
      case e: MqttException => println("Exception Caught: " + e)

    }

    val msgTopic: MqttTopic = client.getTopic(topic)

    val msg: String = "Mqtt publisher test data"

    while (true) {
      val message: MqttMessage = new MqttMessage(String.valueOf(msg).getBytes())
      msgTopic.publish(message);
      println("Published data. Topic: " + msgTopic.getName() + " Message: " + msg)
      Thread.sleep(100);
    }
    client.disconnect();

  }
}
