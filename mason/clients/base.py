
class Client:
    
    def name(self) -> str:
        return self.__class__.__name__.replace("Client", "").lower()
