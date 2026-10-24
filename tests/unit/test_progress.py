from uuid import UUID, uuid4

from e3.runs.progress import NodeProgress, NodeState, RunState, plan

READ, PUBLISH = uuid4(), uuid4()


def chain(read: NodeState, publish: NodeState) -> list[NodeProgress]:
    """`Read → Publish Dataset`."""
    return [
        NodeProgress(READ, read, upstream=frozenset[UUID]()),
        NodeProgress(PUBLISH, publish, upstream=frozenset({READ})),
    ]


def test_fresh_run_starts_only_nodes_without_upstream() -> None:
    result = plan(chain(NodeState.PENDING, NodeState.PENDING))

    assert result.start == [READ]
    assert result.run_state == RunState.RUNNING


def test_node_waits_until_upstream_succeeded() -> None:
    result = plan(chain(NodeState.FINALIZING, NodeState.PENDING))

    assert result.start == []
    assert result.run_state == RunState.RUNNING


def test_succeeded_upstream_releases_downstream() -> None:
    result = plan(chain(NodeState.SUCCEEDED, NodeState.PENDING))

    assert result.start == [PUBLISH]


def test_run_succeeds_when_every_node_succeeded() -> None:
    result = plan(chain(NodeState.SUCCEEDED, NodeState.SUCCEEDED))

    assert result.run_state == RunState.SUCCEEDED
    assert result.start == []


def test_failed_node_skips_pending_nodes_and_fails_run() -> None:
    result = plan(chain(NodeState.FAILED, NodeState.PENDING))

    assert result.skip == [PUBLISH]
    assert result.start == []
    assert result.run_state == RunState.FAILED


def test_run_fails_only_after_active_nodes_finish() -> None:
    other = uuid4()
    nodes = [
        *chain(NodeState.FAILED, NodeState.PENDING),
        NodeProgress(other, NodeState.RUNNING, upstream=frozenset[UUID]()),
    ]

    result = plan(nodes)

    assert result.run_state == RunState.RUNNING
