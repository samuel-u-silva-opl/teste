import pika
import time

def callback(ch, method, properties, body):
    print(f"[x] Mensagem recebida: {body.decode()}")
    time.sleep(1)  # Simula processamento
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='minha_fila', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='minha_fila', on_message_callback=callback)
    print(" [*] Aguardando mensagens.")
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer()
