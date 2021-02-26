from mason.clients.responsable import Responsable
from mason.clients.response import Response


class TableSummary(Responsable):
    
    def __init__(self):
        pass
    
    def to_response(self, response: Response):
        response.add_data({})
        return response
