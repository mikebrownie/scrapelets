from scrapy.exporters import JsonLinesItemExporter
from confluent_kafka import Producer
"""
This pipeline pings kafka with the path of the newly created file after the jsonlines exporters is done 

Usage: First, add to pipelines.py in Scrapy project. Then, enable the pipeline in settings.py. Now you can 
listen to the Kafka feed to process scrapy file outputs.
"""

class KafkaPipeline:
    """Ping kafka with filename after export"""

    @classmethod
    def from_crawler(cls, crawler):
	"""set these in scrapy settings.py"""
        return cls(
            crawler.settings.get("KAFKA_CONFIG"), crawler.settings.get("KAFKA_TOPIC"), crawler.settings.get("KAFKA_KEY")
        )

    def __init__(self, conf, topic, key):
        self.kafka_conf = conf
        self.kafka_topic = topic
        self.kafka_key = key

    def open_spider(self, spider):
        self.file_path = spider.from_date.strftime("%Y-%m-%dT%H-%M-%S") + ".jsonl"
        try:
            self.file = open(self.file_path, "wb+")
        except FileNotFoundError:

            self.logger.error(
                "File not found for export pipeline. "
                "Double check that the output directory exists"
            )
            exit("exiting...")
        self.exporter = JsonLinesItemExporter(self.file)
        self.logger = spider.logger
        self.logger.info(f"Started successfully, writing to {self.file_path}")

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        producer = Producer(self.kafka_conf)
        producer.flush()
        producer.produce(topic=self.kafka_topic, key=self.kafka_key, value=self.file_path)
        producer.flush()
