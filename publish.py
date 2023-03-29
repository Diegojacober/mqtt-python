import random
import time
import mysql.connector

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = "python/mqtt"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'emqx'
password = 'public'

def connect():
        try:
            conection = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='Di29122004#',
                database='mqtt',
                charset='utf8')
        except Exception as e:
            print(f"We can't connected with the server. \033[31m ERROR!: {e} \033[m")
        else:
           return conection

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    db = connect()
    msg_count = 0
    while True:
        time.sleep(1)
        msg = input('Whats your message for everyone?')
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
            db.cursor(buffered=True).execute(f"INSERT INTO publisheds(message) values('{msg}');")
            db.commit()
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()