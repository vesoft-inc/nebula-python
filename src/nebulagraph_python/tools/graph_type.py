from pydantic import BaseModel
from typing_extensions import TYPE_CHECKING, Dict, List, Optional, Self, Type, Union

if TYPE_CHECKING:
    from nebulagraph_python.orm.model import EdgeModel, NodeModel


class PropTypeRow(BaseModel):
    property_name: str
    data_type: str
    nullable: bool
    default: Optional[str]


class WithPropType(BaseModel):
    properties: Dict[str, PropTypeRow]
    pr_or_me_keys: List[str]


class NodeType(WithPropType):
    node_type: str
    labels: List[str]

    def to_gql(self) -> str:
        return f"NODE `{self.node_type}` ({_labels_to_gql(self.labels)} {{ {_props_to_gql(self.properties)} PRIMARY KEY ({', '.join('`' + x + '`' for x in self.pr_or_me_keys)}) }})"


class EdgeType(WithPropType):
    edge_type: str
    src_node_type: str
    dst_node_type: str
    labels: List[str]

    @property
    def edge_pattern(self) -> str:
        return f"({self.src_node_type})-[{self.edge_type}]->({self.dst_node_type})"

    def to_gql(self) -> str:
        return f"EDGE `{self.edge_type}` (`{self.src_node_type}`)-[{_labels_to_gql(self.labels)} {{ {_props_to_gql(self.properties)} MULTIEDGE KEY ({', '.join('`' + x + '`' for x in self.pr_or_me_keys)}) }}]->(`{self.dst_node_type}`)"


class GraphType(BaseModel):
    name: str
    nodes: Dict[str, NodeType]
    edges: Dict[str, EdgeType]

    @classmethod
    def from_models(
        cls,
        graph_type_name: str,
        models: List[Union[Type["NodeModel"], Type["EdgeModel"]]],
    ) -> Self:
        from nebulagraph_python.orm.model import EdgeModel, NodeModel

        nodes = {}
        edges = {}
        for model in models:
            if issubclass(model, NodeModel):
                nodes[model.get_type()] = model.to_type()
            elif issubclass(model, EdgeModel):
                edges[model.get_type()] = model.to_type()
        return cls(name=graph_type_name, nodes=nodes, edges=edges)

    def to_gql(self) -> str:
        return (
            f"CREATE GRAPH TYPE {self.name} {{\n"
            + ",\n".join([x.to_gql() for x in self.nodes.values()])
            + ",\n"
            + ",\n".join([x.to_gql() for x in self.edges.values()])
            + "\n"
            + "}"
        )


def _labels_to_gql(labels: List[str]) -> str:
    return (":" + "&".join(f"`{x}`" for x in labels)) if labels else ""


def _props_to_gql(props: Dict[str, PropTypeRow]) -> str:
    if not props:
        return ""
    return (
        ", ".join(
            [
                f"`{k}` {v.data_type} {'' if v.nullable else 'NOT NULL'} {f'DEFAULT {v.default}' if v.default is not None else ''}"
                for k, v in props.items()
            ]
        )
        + ", "
    )
