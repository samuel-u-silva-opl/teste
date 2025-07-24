import pika
import time
import logging
import sys
import traceback

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.pika import PikaInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# --- Inicializa OpenTelemetry ---
resource = Resource(attributes={"service.name": "rabbitmq-consumer"})

# Tracing
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://openlia_grafana:4317", insecure=True))
trace.get_tracer_provider().add_span_processor(span_processor)

# Métricas
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://openlia_grafana:4317", insecure=True)
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
meter = metrics.get_meter(__name__)
mensagens_processadas = meter.create_counter(
    "mensagens_processadas_total",
    description="Número de mensagens processadas com sucesso"
)

# Instrumenta logging
LoggingInstrumentor().instrument(set_logging_format=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Instrumenta Pika
PikaInstrumentor().instrument()

RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'minha_fila'
RECONNECT_DELAY = 5  # segundos

def callback(ch, method, properties, body):
    with tracer.start_as_current_span("processamento_mensagem") as span:
        try:
            mensagem = body.decode()
            logger.info(f"[x] Mensagem recebida: {mensagem}")
            span.set_attribute("mensagem", mensagem)

            # Simula algum processamento
            time.sleep(1)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("[✓] Mensagem processada e confirmada.")
            mensagens_processadas.add(1)

        except Exception as e:
            logger.error(f"[!] Erro ao processar mensagem: {e}")
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            span.record_exception(e)
            span.set_status(trace.Status(status_code=trace.StatusCode.ERROR, description=str(e)))

def start_consumer():
    while True:
        try:
            logger.info("Conectando ao RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            # Criação da fila do tipo quorum
            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={"x-queue-type": "quorum"}
            )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

            logger.info(" [*] Aguardando mensagens. Pressione CTRL+C para sair.")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"[!] Falha ao conectar ao RabbitMQ: {e}")
            logger.info(f"[↻] Tentando reconectar em {RECONNECT_DELAY} segundos...")
            time.sleep(RECONNECT_DELAY)
        except KeyboardInterrupt:
            logger.info("[x] Encerrando consumidor.")
            try:
                connection.close()
            except:
                pass
            sys.exit(0)
        except Exception as e:
            logger.error(f"[!] Erro inesperado: {e}")
            traceback.print_exc()
            logger.info(f"[↻] Reiniciando em {RECONNECT_DELAY} segundos...")
            time.sleep(RECONNECT_DELAY)

if __name__ == '__main__':
    start_consumer()
