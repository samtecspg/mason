from typistry.protos.proto_object import ProtoObject
from mason.operators.operator import Operator

class OperatorProto(ProtoObject):

    def build_class(self):
        return Operator

