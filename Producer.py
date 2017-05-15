import time
from kafka import KafkaProducer 

class Producer:
	"""docstring for ClassName"""
	def __init__(self, host, port):
		self.producer = KafkaProducer(bootstrap_servers=host+":"+str(port))


	def publicMessage(self, message):
		producer = self.producer
		producer.send('test', message)
		time.sleep(2)