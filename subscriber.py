import random

from paho.mqtt import client as mqtt_client
import mysql.connector

broker = 'broker.emqx.io'
port = 1883
topic = "python/mqtt"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
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

def connect_mqtt() -> mqtt_client:
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


def subscribe(client: mqtt_client):
    db = connect()
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        mensagem = msg.payload.decode()
        db.cursor(buffered=True).execute(f"INSERT INTO receiveds(message) values('{mensagem}');")
        db.commit()
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
