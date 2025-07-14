import pika
import time
import logging

# Configura o log para imprimir no console (stdout), visível no Docker/EasyPanel
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    mensagem = body.decode()
    logger.info(f"[x] Mensagem recebida: {mensagem}")

    # Simula algum processamento
    time.sleep(1)

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info("[✓] Mensagem processada e confirmada.")

def start_consumer():
    logger.info("Conectando ao RabbitMQ...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Criação da fila do tipo quorum
    channel.queue_declare(
        queue='minha_fila',
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )

    # Limita a entrega de uma mensagem por vez
    channel.basic_qos(prefetch_count=1)

    # Define a função que será chamada ao receber uma mensagem
    channel.basic_consume(queue='minha_fila', on_message_callback=callback)

    logger.info(" [*] Aguardando mensagens. Pressione CTRL+C para sair.")
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer()
