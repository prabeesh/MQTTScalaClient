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

    val brokerUrl: String = "tcp://10.30.9.67:1883"
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
    val callback = new MqttCallback {

      override def messageArrived(topic: String, message: MqttMessage): Unit = println(message.toString)

      override def connectionLost(cause: Throwable): Unit = System.err.println("Connection lost" + cause)

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {

      }
    }

    //Set up callback for MqttClient
    client.setCallback(callback)

  }
}
