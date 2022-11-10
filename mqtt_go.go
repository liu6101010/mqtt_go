package main

import (
	"flag"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Message %s received on topic %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection Lost: %s\n", err.Error())
}

func main() {
	broker := flag.String("broker", "tcp://app0.pandavon.com:1883", "The broker URI. ex: tcp://10.10.1.1:1883")
	topic := flag.String("topic", "", "The topic name to/from which to publish/subscribe")
	password := flag.String("password", "", "The password (optional)")
	user := flag.String("user", "", "The User (optional)")
	id := flag.String("id", "testgoid", "The ClientID (optional)")
	cleansess := flag.Bool("clean", false, "Set Clean Session (default false)")
	qos := flag.Int("qos", 1, "The Quality of Service 0,1,2 (default 0)")
	payload := flag.String("message", "", "The message text to publish (default empty)")
	store := flag.String("store", ":memory:", "The Store Directory (default use memory store)")
	flag.Parse()

	if *topic == "" {
		fmt.Println("Invalid setting for -topic, must not be empty")
		fmt.Println("./mqtt_go -broker tcp://10.10.1.1:1883 -user xxx -password xxx -topic xxx")
		return
	}

	fmt.Printf("Sample Info:\n")
	fmt.Printf("\tbroker:    %s\n", *broker)
	fmt.Printf("\tclientid:  %s\n", *id)
	fmt.Printf("\tuser:      %s\n", *user)
	fmt.Printf("\tpassword:  %s\n", *password)
	fmt.Printf("\ttopic:     %s\n", *topic)
	fmt.Printf("\tmessage:   %s\n", *payload)
	fmt.Printf("\tqos:       %d\n", *qos)
	fmt.Printf("\tcleansess: %v\n", *cleansess)
	fmt.Printf("\tstore:     %s\n", *store)

	//var broker = "tcp://app0.pandavon.com:1883"
	options := mqtt.NewClientOptions()
	options.AddBroker(*broker)
	options.SetClientID("go_mqtt_example")
	options.SetUsername(*user)
	options.SetPassword(*password)
	options.SetDefaultPublishHandler(messagePubHandler)
	options.OnConnect = connectHandler
	options.OnConnectionLost = connectionLostHandler

	options.SetCleanSession(*cleansess)
	//options.SetKeepAlive(60)
	////自动重连机制，如网络不稳定可开启
	options.SetAutoReconnect(true)      //启用自动重连功能
	options.SetMaxReconnectInterval(30) //每30秒尝试重连
	if *store != ":memory:" {
		options.SetStore(mqtt.NewFileStore(*store))
	}

	client := mqtt.NewClient(options)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	topic_sub := "liulei/rpc"
	token = client.Subscribe(topic_sub, byte(*qos), nil)
	token.Wait()
	fmt.Printf("Subscribed to topic %s\n", topic_sub)

	num := 10
	for i := 0; i < num; i++ {
		text := fmt.Sprintf("%d", i)
		token = client.Publish(*topic, byte(*qos), false, text)
		token.Wait()
		time.Sleep(time.Second)
	}
	client.Unsubscribe(topic_sub)

	client.Disconnect(100)
}
