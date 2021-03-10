
class Client:
    
    def name(self) -> str:
        return self.__class__.__name__.replace("Client", "").lower()

    def to_dict(self) -> dict:
        return {'name': self.name()}