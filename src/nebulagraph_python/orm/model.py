from dataclasses import dataclass
from datetime import date
from types import UnionType
from typing import Iterable
from collections.abc import Sequence as ABCSequence

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    TypeAdapter,
)
from pydantic.fields import FieldInfo
from typing_extensions import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Self,
    Sequence,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from nebulagraph_python.client.base_executor import unwrap_props
from nebulagraph_python.py_data_types import (
    BasicTargetType,
    EdgePrimitive,
    NodePrimitive,
    NVector,
)
from nebulagraph_python.tools.graph_type import (
    EdgeType,
    GraphType,
    NodeType,
    PropTypeRow,
)

T = TypeVar("T")


class NodeModel(BaseModel):
    model_config = ConfigDict(revalidate_instances="always")

    def get_element_id(self) -> Optional[int]: ...

    @classmethod
    def get_type(cls) -> str: ...

    @classmethod
    def get_labels(cls) -> Sequence[str]: ...

    def get_properties(self) -> Mapping[str, Any]:
        return get_props(self)

    def get_prop(self, key: str) -> tuple[str, BasicTargetType]:
        return get_prop(self, key)

    @classmethod
    def get_primary_keys(cls) -> List[str]: ...

    def get_primary_kv(self) -> Dict[str, BasicTargetType]:
        return dict(self.get_prop(k) for k in self.get_primary_keys())

    @classmethod
    def from_primitive(cls, data: NodePrimitive) -> Self: ...

    @classmethod
    def to_type(cls) -> NodeType: ...


class EdgeModel(BaseModel):
    model_config = ConfigDict(revalidate_instances="always")

    def get_src_element_id(self) -> Optional[int]: ...

    def get_dst_element_id(self) -> Optional[int]: ...

    def get_rank(self) -> Optional[int]:
        """rank == 0 means not given by input,

        rank == None means no multiedge key specified by the edge type"""
        ...

    @classmethod
    def get_type(cls) -> str: ...

    @classmethod
    def get_src_type(cls) -> str: ...

    @classmethod
    def get_dst_type(cls) -> str: ...

    @classmethod
    def get_labels(cls) -> Sequence[str]: ...

    def get_properties(self) -> Mapping[str, Any]:
        return get_props(self)

    def get_prop(self, key: str) -> tuple[str, BasicTargetType]:
        return get_prop(self, key)

    @classmethod
    def get_multiedge_keys(cls) -> List[str]: ...

    def get_multiedge_kv(self) -> Dict[str, BasicTargetType]:
        return dict(self.get_prop(k) for k in self.get_multiedge_keys())

    @classmethod
    def from_primitive(cls, data: EdgePrimitive) -> Self: ...

    @classmethod
    def to_type(cls) -> EdgeType: ...


def node_model(
    type: str,
    primary_key: Union[str, List[str]],
    labels: Optional[Sequence[str]] = None,
) -> Type[NodeModel]:
    """Dynamically create a Node type based on the given type and labels"""
    if labels is None:
        labels = []
    if isinstance(primary_key, str):
        primary_key = [primary_key]

    class NodeModelDynamic(NodeModel):
        # Class variables - For NebulaGraph Type
        nebula_type__type__: ClassVar[str] = type
        nebula_type__primary_key__: ClassVar[List[str]] = primary_key
        nebula_type__labels__: ClassVar[Sequence[str]] = labels

        # Instance variable - For NebulaGraph Object but not properties
        nebula_obj__element_id__: Optional[int] = Field(default=None, exclude=True)

        @classmethod
        def from_primitive(cls, data: NodePrimitive) -> Self:
            return cls.model_validate(
                {
                    "nebula_obj__element_id__": data["id"],
                    **data["properties"],
                }
            )

        def get_element_id(self):
            return self.nebula_obj__element_id__

        @classmethod
        def get_type(cls):
            return cls.nebula_type__type__

        @classmethod
        def get_labels(cls):
            return cls.nebula_type__labels__

        @classmethod
        def get_primary_keys(cls):
            return cls.nebula_type__primary_key__

        @classmethod
        def to_type(cls):
            return node_model_to_type(cls)

    return NodeModelDynamic


def edge_model(
    src_type: str,
    edge_type: str,
    dst_type: str,
    labels: Optional[Sequence[str]] = None,
    multiedge_keys: Optional[List[str]] = None,
) -> Type[EdgeModel]:
    """Dynamically create an Edge type based on the given type information"""
    if labels is None:
        labels = []
    multiedge_keys = multiedge_keys or []

    class EdgeModelDynamic(EdgeModel):
        # Class variables - For NebulaGraph Type
        nebula_type__src_type__: ClassVar[str] = src_type
        nebula_type__edge_type__: ClassVar[str] = edge_type
        nebula_type__dst_type__: ClassVar[str] = dst_type
        nebula_type__labels__: ClassVar[Sequence[str]] = labels
        nebula_type__multiedge_key__: ClassVar[List[str]] = multiedge_keys

        # Instance variable - For NebulaGraph Object but not properties
        nebula_obj__src_element_id__: Optional[int] = Field(default=None, exclude=True)
        nebula_obj__dst_element_id__: Optional[int] = Field(default=None, exclude=True)
        nebula_obj__rank__: Optional[int] = Field(default=None, exclude=True)

        @classmethod
        def from_primitive(cls, data: EdgePrimitive) -> Self:
            return cls.model_validate(
                {
                    "nebula_obj__src_element_id__": data["src_id"],
                    "nebula_obj__dst_element_id__": data["dst_id"],
                    "nebula_obj__rank__": data["rank"],
                    **data["properties"],
                }
            )

        def get_src_element_id(self):
            return self.nebula_obj__src_element_id__

        def get_dst_element_id(self):
            return self.nebula_obj__dst_element_id__

        def get_rank(self):
            return self.nebula_obj__rank__

        @classmethod
        def get_type(cls):
            return cls.nebula_type__edge_type__

        @classmethod
        def get_src_type(cls):
            return cls.nebula_type__src_type__

        @classmethod
        def get_dst_type(cls):
            return cls.nebula_type__dst_type__

        @classmethod
        def get_labels(cls):
            return cls.nebula_type__labels__

        @classmethod
        def get_multiedge_keys(cls):
            return cls.nebula_type__multiedge_key__

        @classmethod
        def to_type(cls):
            return edge_model_to_type(cls)

    return EdgeModelDynamic


def bind_node(
    type_desc: Union[
        GraphType,
        NodeType,
    ],
    name: Optional[str] = None,
):
    if isinstance(type_desc, GraphType):
        if not name:
            raise ValueError("name is required for GraphType")
        type_desc = type_desc.nodes[name]
    elif isinstance(type_desc, NodeType):
        pass
    else:
        raise ValueError(f"Invalid type description: {type_desc}")

    return node_model(
        type_desc.node_type,
        type_desc.pr_or_me_keys,
        type_desc.labels,
    )


def bind_edge(
    type_desc: Union[
        GraphType,
        EdgeType,
    ],
    name: Optional[str] = None,
):
    if isinstance(type_desc, GraphType):
        if not name:
            raise ValueError("name is required for GraphType")
        type_desc = type_desc.edges[name]
    elif isinstance(type_desc, EdgeType):
        pass
    else:
        raise ValueError(f"Invalid type description: {type_desc}")
    return edge_model(
        type_desc.src_node_type,
        type_desc.edge_type,
        type_desc.dst_node_type,
        type_desc.labels,
        type_desc.pr_or_me_keys,
    )


def _node_expr(node: NodeModel) -> str:
    return f"(@`{node.get_type()}` {{ {unwrap_props(node.get_properties())} }})"


def _edge_expr(edge: EdgeModel) -> str:
    return f"[@`{edge.get_type()}` {{ {unwrap_props(edge.get_properties())} }}]"


def upsert_batch_nodes_gql(
    items: Sequence[NodeModel],
    batch_size: int = 1024,
) -> Iterable[str]:
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        batch_str = ", ".join(_node_expr(node) for node in batch)
        yield f"INSERT OR UPDATE {batch_str}"


def upsert_gql(
    node: NodeModel,
    edge: Optional[EdgeModel] = None,
    dst_node: Optional[NodeModel] = None,
):
    """Upsert a node or a triplet (node, edge, dst_node), and return the GQL string"""
    if edge is None:
        return f"""INSERT OR UPDATE {_node_expr(node)}"""
    else:
        if dst_node is None:
            raise ValueError("dst_node is required when edge is provided")
        return f"""INSERT OR UPDATE {_node_expr(node)}-{_edge_expr(edge)}->{_node_expr(dst_node)}"""


def upsert_edge_only_gql(
    src: Union[int, Dict[str, Any], NodeModel],
    edge: EdgeModel,
    dst: Union[int, Dict[str, Any], NodeModel],
) -> str:
    """Take in the element_id or the properties filter for the source and destination nodes,
    and return the GQL string for the edge upsert"""

    def _get_pattern(
        idt: str, type_: str, element: Union[int, Dict[str, Any], NodeModel]
    ) -> str:
        base = f"({idt}@`{type_}`"
        if isinstance(element, int):
            return f"{base} WHERE element_id({idt}) = {element} )"
        elif isinstance(element, NodeModel):
            return f"{base} {{ {unwrap_props(element.get_primary_kv())} }} )"
        else:
            return f"{base} {{ {unwrap_props(element)} }} )"

    src_pattern = _get_pattern("u", edge.get_src_type(), src)
    dst_pattern = _get_pattern("v", edge.get_dst_type(), dst)
    return f"""MATCH {src_pattern}, {dst_pattern} INSERT OR UPDATE (u)-{_edge_expr(edge)}->(v)"""


def _check_optional(t: Any) -> tuple[bool, type]:
    """
    Check if a type annotation is Optional[T], Union[T, None], or T | None.
    Returns a tuple of (is_optional, inner_type).

    Args:
        t: A type annotation, typically from a pydantic model_field.annotation

    Returns:
        A tuple of (is_optional, inner_type) where:
            - is_optional: True if the type is optional, False otherwise
            - inner_type: The underlying type T (without the Optional wrapper)
    """
    # First, handle the case where t might be None itself
    if t is None:
        raise ValueError("t is None")

    # Get origin and args - this works for both Python 3.8+ typing structures
    origin = get_origin(t)
    args = get_args(t)

    # Handle both Union, type_extensions.Optional and the PEP 604 | operator
    if origin is Union or origin is UnionType:
        if len(args) == 2:
            if (arg0 := args[0]) is type(None) or (arg1 := args[1]) is type(None):
                return True, arg0 if arg0 is not type(None) else arg1
            else:
                raise ValueError("Union type should have exactly one None type")
        else:
            raise ValueError("Union type should have exactly two types")

    # Handle standard types (not optional)
    return False, t


DATA_TYPE_MAPPING = {
    int: "INT64",
    float: "FLOAT64",
    str: "STRING",
    bool: "BOOL",
    date: "DATE",
}
DATA_TYPE_KEYS = [*DATA_TYPE_MAPPING.keys(), NVector]


@dataclass(frozen=True)
class AdditionalMetadata:
    """Annotation for that in BasicTargetType but not primitive type"""

    name: Literal["AsJSON", "AsNVector"]
    data: Any


class AsJSON(Protocol):
    def __class_getitem__(cls, T: type):
        if T is str:
            raise ValueError("AsJSON does not support str as the inner type")

        return Annotated[
            T,
            BeforeValidator(
                lambda v: (TypeAdapter(T).validate_json(v) if isinstance(v, str) else v)
            ),
            PlainSerializer(lambda v: TypeAdapter(T).dump_json(v).decode("utf-8")),
            AdditionalMetadata(name="AsJSON", data=None),
        ]


class AsNVector(Protocol):
    """Adapter utilities for Nebula vectors with serialization helpers."""

    @staticmethod
    def load(value: Sequence[float] | NVector) -> NVector:
        """Coerce plain sequences into NVectors to simplify model loading."""

        if isinstance(value, NVector):
            return value
        if isinstance(value, (str, bytes)):
            raise TypeError("NVector expects a numeric sequence, got string-like input")
        if isinstance(value, ABCSequence):
            return NVector(values=[float(component) for component in value])
        raise TypeError(f"Cannot coerce value of type {type(value)!r} into NVector")

    @staticmethod
    def dump(value: NVector) -> list[float]:
        """Convert an NVector into a plain list for persistence/transport."""

        return list(value.get_values())

    @staticmethod
    def save(value: Sequence[float] | NVector) -> list[float]:
        """Coerce the provided value and dump it in one call for convenience."""

        return AsNVector.dump(AsNVector.load(value))

    def __class_getitem__(cls, size: int):
        def after_validator(v: NVector) -> NVector:
            if v.dimension != size:
                raise AssertionError(
                    f"Vector dimension mismatch: got {v.dimension} dim but the type should be dim {size}"
                )
            return v

        return Annotated[
            NVector,
            BeforeValidator(AsNVector.load),
            AfterValidator(after_validator),
            PlainSerializer(AsNVector.dump),
            AdditionalMetadata(name="AsNVector", data=size),
        ]


def _scan_metadata(t: type, field_info: FieldInfo) -> Optional[AdditionalMetadata]:
    for meta in [*getattr(t, "__metadata__", []), *field_info.metadata]:
        if isinstance(meta, AdditionalMetadata):
            return meta
    return None


def get_data_type(t: type, field_info: FieldInfo) -> str:
    if t in DATA_TYPE_MAPPING:
        return DATA_TYPE_MAPPING[t]
    else:
        # For list and List
        if get_origin(t) is list:
            inner_type = get_args(t)[0]
            try:
                inner_type_name = DATA_TYPE_MAPPING[inner_type]
            except KeyError as e:
                raise ValueError(
                    f"Unsupported inner type: {inner_type} for List"
                ) from e
            return f"LIST<{inner_type_name}>"

        # For AsNVector and AsJSON
        meta = _scan_metadata(t, field_info)
        if meta is not None:
            if meta.name == "AsNVector":
                return f"VECTOR<{meta.data}, FLOAT>"
            elif meta.name == "AsJSON":
                return "STRING"
            else:
                raise NotImplementedError(f"Unsupported metadata: {meta.name}")
        else:
            raise ValueError(
                f"No metadata found for the field {field_info}, which is not a primitive type."
            )


def get_prop(obj: NodeModel | EdgeModel, key: str) -> tuple[str, BasicTargetType]:
    field = obj.__class__.model_fields[key]

    def _get_alias() -> str:
        if field.alias:
            return field.alias
        else:
            return key

    _, t = _check_optional(field.annotation)
    meta = _scan_metadata(t, field)
    # For mannual defined case
    if meta is not None and meta.name == "AsNVector":
        v = getattr(obj, key)
        return (_get_alias(), v)

    # For pydantic handeled case
    v = obj.model_dump(
        include={
            key,
        }
    )[key]
    return (_get_alias(), v)


def get_props(obj: NodeModel | EdgeModel) -> Dict[str, BasicTargetType]:
    return dict(
        get_prop(obj, k) for k, v in obj.__class__.model_fields.items() if not v.exclude
    )


def _encode_properties(props: Mapping[str, FieldInfo]):
    res: Dict[str, PropTypeRow] = {}
    for k, v in props.items():
        if k.startswith("nebula_obj__"):
            continue
        name = v.alias or k
        is_optional, inner_type = _check_optional(v.annotation)
        res[name] = PropTypeRow(
            property_name=name,
            data_type=get_data_type(inner_type, v),
            nullable=is_optional,
            default=None,
        )

    return res


def node_model_to_type(node: Type[NodeModel]) -> NodeType:
    pk = node.get_primary_keys()
    props = _encode_properties(node.model_fields)
    return NodeType(
        node_type=node.get_type(),
        labels=list(node.get_labels()),
        properties=props,
        pr_or_me_keys=pk,
    )


def edge_model_to_type(edge: Type[EdgeModel]) -> EdgeType:
    mek = edge.get_multiedge_keys()
    props = _encode_properties(edge.model_fields)
    return EdgeType(
        edge_type=edge.get_type(),
        src_node_type=edge.get_src_type(),
        dst_node_type=edge.get_dst_type(),
        labels=list(edge.get_labels()),
        properties=props,
        pr_or_me_keys=mek,
    )
