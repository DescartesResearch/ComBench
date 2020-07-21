import abc


class ProtocolAdapter(abc.ABC):

    @abc.abstractmethod
    def start_client(self):
        pass

    @abc.abstractmethod
    def subscribe(self, topic):
        pass

    @abc.abstractmethod
    def publish(self):
        pass

    @abc.abstractmethod
    def stop_client(self):
        pass

    @abc.abstractmethod
    def add_callback(self):
        pass
