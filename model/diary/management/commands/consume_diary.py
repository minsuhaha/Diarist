from django.core.management.base import BaseCommand
from kafka_consumer import consume

class Command(BaseCommand):
    help = 'Consume diary messages from Kafka'

    def handle(self, *args, **options):
        consume()
