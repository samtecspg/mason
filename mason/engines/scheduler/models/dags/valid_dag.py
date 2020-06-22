import tempfile
from typing import List, Tuple, Optional

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.resources.asciidag.graph import Graph
from mason.resources.asciidag.node import Node
from mason.util.list import flatten_array, flatten

class ValidDag:

    def __init__(self, namespace: str, command: str, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.namespace = namespace
        self.command = command
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps
        
    def get_next_steps(self, valid_dag_step: ValidDagStep) -> List[ValidDagStep]:
        step_id: str = valid_dag_step.id
        next_step_ids = self.step(step_id)
        not_current = [s for s in next_step_ids if s != step_id]
        return flatten(list(map(lambda s: self.get_node(s), not_current)))

    def edges(self) -> List[Tuple[str, str]]:
        edges: List[Tuple[str, str]] = []
        for step in self.valid_steps:
            for dep in step.dependencies:
                if dep in list(map(lambda s: s.id, self.valid_steps)):
                    edges.append((dep, step.id))
                
        return edges
    
    def reverse_edges(self) -> List[Tuple[str, str]]:
        return list(map(lambda e: (e[1], e[0]),self.edges()))
            
    def roots(self) -> List[ValidDagStep]:
        roots = [v for v in self.valid_steps if (len(v.dependencies) == 0)]
        return list(set(roots))
    
    def root_ids(self) -> List[str]:
        return list(map(lambda r: r.id, self.roots()))
    
    def get_node(self, node_id: str) -> Optional[ValidDagStep]:
        return next((x for x in self.valid_steps if x.id == node_id), None)
        
    def step(self, node: str, backwards: Optional[bool] = None) -> List[str]:
        back = backwards or False
        if back:
            edges = self.reverse_edges()
        else:
            edges = self.edges()
            
        matching_edges = [v[1] for v in edges if v[0] == node] 
        if len(matching_edges) == 0:
            return [node]
        else:
            return matching_edges
    
    def terminal_nodes(self) -> List[str]:
        roots = sorted(self.root_ids())
        last = roots 
        next = sorted(flatten_array(list(map(lambda n: self.step(n), last))))
        while (next != last):
            last = next
            next = sorted(flatten_array(list(map(lambda n: self.step(n), last))))
        return next
    
    #  TODO:  Handles two parents with one child. Does not yet handle one parent with two children (well). 
    def step_node(self, node: Node) -> List[Node]:
        node_names = self.step(node.item, True)
        if node_names != [node.item]:
            return list(map(lambda n: Node(n, parents=[node]), node_names))
        else:
            return [node]
    
    def get_nodes(self):
        terminal_nodes = list(set(self.terminal_nodes()))
        terminal: List[Node]= list(map(lambda t: Node(t), terminal_nodes))

        last: List[str] = [] 
        new_nodes = terminal
        next: List[str] = sorted(list(map(lambda n: n.item, new_nodes)))

        while (next != last):
            last = next
            nn = flatten_array(list(map(lambda n: self.step_node(n), new_nodes)))
            next = sorted(list(map(lambda n: n.item, nn)))
            if next != last:
                new_nodes = nn

        return new_nodes
    
    def display(self) -> str:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp:
            graph = Graph(fh=temp)
            nodes = self.get_nodes()
            graph.show_nodes(nodes)
                
        with open(temp.name) as f:
            f.seek(0)
            string = "".join(f.readlines())
                
        return string