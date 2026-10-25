"""Граф пайплайна: слоты, узлы BLOCK и SYSTEM, рёбра между портами.

Форма графа проверяется при сохранении Draft, смысл — при публикации
(см. `validation`).
"""

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

PUBLISH_INPUT_PORT = "data"


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExactVersion(_Frozen):
    """Значение Input Slot по умолчанию: точная DatasetVersion."""

    dataset_id: UUID
    version_id: UUID


class LatestVersion(_Frozen):
    """Значение Input Slot по умолчанию: последняя версия Dataset на момент запуска."""

    dataset_id: UUID
    version: Literal["latest"]


class TargetDataset(_Frozen):
    """Значение Output Slot по умолчанию: целевой Dataset."""

    dataset_id: UUID


class InputSlot(_Frozen):
    name: str = Field(min_length=1)
    default: ExactVersion | LatestVersion | None = None


class OutputSlot(_Frozen):
    name: str = Field(min_length=1)
    default: TargetDataset | None = None


class BlockRef(_Frozen):
    """Точная версия BlockSpec."""

    name: str
    version: str

    # Pydantic и так хеширует frozen-модели, но pyright этого не видит.
    def __hash__(self) -> int:
        return hash((self.name, self.version))


class BlockNode(_Frozen):
    kind: Literal["BLOCK"]
    id: str = Field(min_length=1)
    block: BlockRef
    parameters: dict[str, Any] = Field(default_factory=dict[str, Any])


class PublishDatasetNode(_Frozen):
    """System Node: публикует то, что пришло на порт `data`, в Dataset Output Slot."""

    kind: Literal["SYSTEM"]
    id: str = Field(min_length=1)
    system: Literal["publish_dataset"]
    output_slot: str


Node = Annotated[BlockNode | PublishDatasetNode, Field(discriminator="kind")]


class NodePort(_Frozen):
    node: str
    port: str


class InputSlotRef(_Frozen):
    slot: str


class Edge(_Frozen):
    source: NodePort | InputSlotRef
    target: NodePort


class PipelineGraph(_Frozen):
    input_slots: list[InputSlot] = Field(default_factory=list[InputSlot])
    output_slots: list[OutputSlot] = Field(default_factory=list[OutputSlot])
    nodes: list[Node] = Field(default_factory=list[Node])
    edges: list[Edge] = Field(default_factory=list[Edge])

    def block_refs(self) -> set[BlockRef]:
        return {node.block for node in self.nodes if isinstance(node, BlockNode)}
