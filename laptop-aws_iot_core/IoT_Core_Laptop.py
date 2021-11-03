import time
import json
import MacTmp
import threading
import subprocess
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder

decoded_msg = None
received_count = 0
message_received = 0
received_all_event = threading.Event()

# Define client_id, path_to_key, path_to_root, path_to_cert,
# endpoint and number_of_messages
client_id = "test-hw1"
path_to_key = "certs/private.pem.key"
path_to_root = "certs/AmazonRootCA1.pem"
path_to_cert = "certs/certificate.pem.crt"
endpoint = "endpoint_id-ats.iot.us-west-2.amazonaws.com"

number_of_messages = 5 # Number of cpu temperature and fan speed recordings

#Topics
topic = "laptop/cputemp"
topic_republish = "laptop/hightemp"
topic_fanspeed = "laptop/getspeed"

# Callback when the subscribed topic receives a message
# Step 3: Receive republished message from new topic
def on_message_received_repub(topic, payload, dup, qos, retain, **kwargs):
    print("\nReceived message from topic '{}': {}".
        format(topic, payload.decode("utf-8")))
    global received_count
    received_count += 1
    if received_count == number_of_messages:
        received_all_event.set()

def on_message_received_cpufan(topic, payload, dup, qos, retain, **kwargs):
    global decoded_msg
    decoded_msg = payload.decode("utf-8")
    global message_received
    message_received += 1
    #print("\nReceived message from topic '{}': {}".format(topic, payload))

if __name__ == '__main__':
    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    proxy_options = None

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
                endpoint=endpoint,
                cert_filepath=path_to_cert,
                pri_key_filepath=path_to_key,
                client_bootstrap=client_bootstrap,
                ca_filepath=path_to_root,
                client_id=client_id,
                port=None,
                clean_session=True,
                keep_alive_secs=50,
                http_proxy_options=None)

    print("Connecting to {} with client ID '{}'...".format(endpoint, client_id))
    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Subscribe to topics
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=None)

    subscribe_future_repub, packet_id = mqtt_connection.subscribe(
        topic=topic_republish,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received_repub)

    subscribe_future_getspeed, packet_id = mqtt_connection.subscribe(
      topic=topic_fanspeed,
      qos=mqtt.QoS.AT_LEAST_ONCE,
      callback=on_message_received_cpufan)

    print ("\nSending {} message(s)".format(number_of_messages))

    # Step 2: Publish message to AWS Iot console
    publish_count = 1
    while (publish_count <= number_of_messages) or (number_of_messages == 0):
        temp = MacTmp.CPU_Temp()
        message = {"Temperature": temp}
        print("\n[{}] Publishing message to topic '{}': {}".
            format(publish_count, topic, message))
        message_json = json.dumps(message)
        mqtt_connection.publish(
            topic=topic,
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(2)
        publish_count += 1

    # Step 4: Receive user command from AWS Iot console and publish new message
    # on arrival.
    if not message_received:
        print("-------------------------------------------")
        print("\nPlease send user command from aws iot console")
        time.sleep(10) # wait for user commands from AWS Iot core console
        if message_received:
            print("\nReceived message from topic '{}': {}".
                format(topic_fanspeed, decoded_msg))
            publish_count = 1
            while (publish_count <= number_of_messages) or (number_of_messages == 0):
                # Publish real-time cpu temperature and cpu fan speed
                temp = float(MacTmp.CPU_Temp())
                speed = subprocess.check_output("sudo powermetrics -i 2000 --samplers smc | grep -m 1 Fan", shell=True)
                speed = str((speed.decode("utf-8")).strip("Fan: ").strip("\n"))
                message = {"Temperature": temp, "Speed": speed}
                print("\n[{}] Publishing message to topic '{}': {}".
                    format(publish_count, topic_fanspeed, message))
                message_json = json.dumps(message)
                mqtt_connection.publish(
                    topic=topic_fanspeed,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE)
                time.sleep(2) # Collect data for every 2 seconds
                publish_count += 1
            # Disconnect
            disconnect_future = mqtt_connection.disconnect()
            disconnect_future.result()
            print("\nDisconnected!")

        if not message_received: # No user command received by laptop
            print("No message received ...")
            # Disconnect
            disconnect_future = mqtt_connection.disconnect()
            disconnect_future.result()
            print("\nDisconnected!")
