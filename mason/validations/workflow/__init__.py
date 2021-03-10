from typistry.protos.proto_object import ProtoObject
from mason.workflows.workflow import Workflow

class WorkflowProto(ProtoObject):

    def build_class(self):
        return Workflow
