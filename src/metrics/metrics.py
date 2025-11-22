class Metrics:
    def __init__(self):
        self.sent_messages = 0
        self.publish_latencies = []

    def inc_sent(self):
        self.sent_messages += 1

    def add_latency(self, value: float):
        self.publish_latencies.append(value)
