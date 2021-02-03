from mason.util.string import to_snake_case

class Client:
    
    def client_name(self):
        return to_snake_case(self.__class__.__name__.replace("Client", ""))

    def to_dict(self):
        return {}

