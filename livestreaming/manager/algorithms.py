import asyncio
from typing import Optional
from typing import Tuple
from typing import Dict
from typing import List
from typing import TYPE_CHECKING
from functools import reduce

from livestreaming.settings import SettingsSectionBase

if TYPE_CHECKING:
    from .streams import ManagerStream
    from .node_types import ContentNode


ToContentNodesType = Dict["ContentNode", int]  # maximum clients on this content node per stream
StreamToContentType = Dict["ManagerStream", ToContentNodesType]


class CannotAssignContentNodesToStreamError(Exception):
    pass


class EncoderToContentReturnStatus:
    def __init__(self, clients_left_out: int, stream_to_content: Optional[StreamToContentType] = None):
        self.clients_left_out: int = clients_left_out  # need more content nodes for n clients
        self.stream_to_content: Optional[StreamToContentType] = stream_to_content


class EncoderToContentAlgorithmBase:
    async def solve(self,
                    streams: List["ManagerStream"],
                    contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        max_viewers = reduce(lambda c, n: c + n.max_clients, contents, 0)
        clients_sum = reduce(lambda c, s: c + s.get_estimated_viewers(), streams, 0)
        if max_viewers < clients_sum:
            return EncoderToContentReturnStatus(clients_left_out=clients_sum-max_viewers)
        else:
            return await self._solve(streams, contents)

    async def _solve(self,
                     streams: List["ManagerStream"],
                     contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        NotImplementedError()


class RecursiveEncoderToContentAlgorithm(EncoderToContentAlgorithmBase):
    """
    This is a simple algorithm which assigns viewers by free space on content nodes.
    There is no guarantee that it will produce optimal results in any sense, in fact one can assume to get the worst.

    Current assignments will not be changed, though.
    """
    async def _solve(self,
                     streams: List["ManagerStream"],
                     contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        # sort nodes by free places
        contents_sorted = sorted(contents, key=lambda c: c.max_clients - c.current_clients, reverse=True)
        content_assignable = {}
        for content in contents_sorted:
            # make a list of free spaces, which can be assigned to streams
            free = content.max_clients - content.current_clients
            if free > 0:
                content_assignable[content] = free

        def r_assign(viewers: int) -> (Optional["ContentNode"], int):
            # recursively go through nodes and find one with enough space
            if viewers <= 1:
                # avoid infinite loops
                return None, viewers
            for content_node, max_viewers in content_assignable:
                if max_viewers >= viewers:
                    return content_node, viewers
            # if none is found try to assign less viewers
            return r_assign(int(viewers / 2))

        result = EncoderToContentReturnStatus(clients_left_out=0)
        result.stream_to_content = {}

        for stream in sorted(streams, key=lambda s: s.get_estimated_viewers(), reverse=True):
            unassigned_clients = stream.get_estimated_viewers() - stream.viewers
            stream_contents: ToContentNodesType = dict(
                (node, obj.current_clients) for node, obj in stream.contents.items())
            while unassigned_clients > 1:
                # keep it going until 1, loop may be infinite else
                node, assigned_clients = r_assign(unassigned_clients)
                if node is not None:
                    if node in stream_contents:
                        stream_contents[node] += assigned_clients
                    else:
                        stream_contents[node] = assigned_clients
                    content_assignable[node] -= assigned_clients
                    unassigned_clients -= assigned_clients
                else:
                    msg = f"Could not find assignment for stream {stream.stream_id} " \
                        f"({stream.get_estimated_viewers()} viewers)"
                    raise CannotAssignContentNodesToStreamError(msg)
            result.stream_to_content[stream] = stream_contents

        return result


class EncoderToContentFlowAlgorithm(EncoderToContentAlgorithmBase):
    """
    TODO
    Solve the assignment problem by a maximum flow problem.

    It is not expected to find an optimal solution in any sense, but due to the nature of MaxFlow algorithms
    it might find a result more close to optimum.

    Expect current assigment to change dramatically.

    async def _solve(self, streams: List["ManagerStream"], contents: List["ContentNode"]):
        k = len(streams)
        n = k + len(contents)
        matrix = [[0] * n] * n
        for i in range(k):
            for j in range(k, n):
                for node in streams[i].contents:
                    if node is contents[j-k]:
                        matrix[i][j] = 0    # node -> stream (for backwards edges)
                        matrix[j][i] = 0    # stream -> node
    """


class OptimalEncoderToContentAlgorithm(EncoderToContentAlgorithmBase):
    """
    Solving the assignment problem (least content node use by stream) by an integer program.
    Due to the complexity of solving such problems (NP-hard), one cannot assume to find an optimal solution quick.
    Therefore a time limit is set for the solver, in order to find at least on solution (likely not optimal).

    However, always choose a time limit in which you expect to find at least one solution (estimated >10s).
    The higher the time one is giving, the closer the solution will be to optimum.

    Expect current assignments to be changed dramatically.
    """
    class OptimizationSettings(SettingsSectionBase):
        _section = "optimization"
        glpk_tmlim: int
        distribution_coefficient: float
        additional_clients_per_stream: int

    _opt_settings: Optional[OptimizationSettings] = None

    @property
    def opt_settings(self):
        return self._opt_settings

    async def _solve(self,
                     streams: List["ManagerStream"],
                     contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        import pulp
        if self._opt_settings is None:
            self._opt_settings = self.OptimizationSettings()
            self._opt_settings.load()
        prob = pulp.LpProblem(sense=pulp.LpMinimize)
        x_vars: Dict[Tuple[ContentNode, ManagerStream], pulp.LpVariable] = {}
        p_vars: Dict[Tuple[ContentNode, ManagerStream], pulp.LpVariable] = {}
        c_vars: Dict[ContentNode, pulp.LpVariable] = {}
        for i in contents:
            c_vars[i] = pulp.LpVariable(f"C_{i.id}", cat=pulp.LpBinary)
            for e in streams:
                x_vars[i, e] = pulp.LpVariable(f"X_{i.id}_{e.stream_id}", lowBound=0)  # reduce complexity
                p_vars[i, e] = pulp.LpVariable(f"P_{i.id}_{e.stream_id}", cat=pulp.LpBinary)
        # minimize:
        prob += pulp.lpSum([p_vars[i, e] for i in contents for e in streams]
                           + [self._opt_settings.distribution_coefficient * c_vars[i] for i in contents])
        # in respect to:
        for i in contents:
            prob += i.max_clients * c_vars[i] - pulp.lpSum([x_vars[i, e] for e in streams]) >= 0
            for e in streams:
                prob += e.viewers * p_vars[i, e] - x_vars[i, e] >= 0
        for e in streams:
            prob += pulp.lpSum([x_vars[i, e] for i in contents]) - e.get_estimated_viewers() >= 0

        # solve
        solver = pulp.GLPK_CMD(msg=0, options=["--tmlim", self._opt_settings.glpk_tmlim])
        await asyncio.get_event_loop().run_in_executor(None, lambda: prob.solve(solver))
        result = EncoderToContentReturnStatus(clients_left_out=0)
        """# TODO
        if prob.status == pulp.LpStatusOptimal:
            result.stream_to_content = []
            stream_to_content: Dict[int, ContentNodeListType] = {}
            for i, e in [(i, e) for i in contents for e in streams]:
                if pulp.value(p_vars[i, e]) == 1:
                    try:
                        val = int(pulp.value(x_vars[(i, e)]))
                    except ValueError:
                        val = 0
                    if e.stream_id not in stream_to_content:
                        stream_to_content[e.stream_id] = []
                    stream_to_content[e.stream_id].append((i.id, val))
            for stream_id, links in stream_to_content.items():
                result.stream_to_content.append((stream_id, links))
        """

        return result


class MToNEncoderToContentAlgorithm(EncoderToContentAlgorithmBase):
    """
    Very simple algorithm: Every stream is available on all content nodes.

    Current assignments will not be changed, since every streams will link to every node.
    """
    async def _solve(self,
                     streams: List["ManagerStream"],
                     contents: List["ContentNode"]) -> EncoderToContentReturnStatus:
        # Distribute all streams on all content nodes.
        stream_to_content: StreamToContentType = {}
        for stream in streams:
            all_contents: ToContentNodesType = {}
            for content in contents:
                all_contents[content] = -1
            stream_to_content[stream] = all_contents

        total_max_clients = reduce(lambda c, n: c + n.max_clients, contents, 0)
        current_clients = reduce(lambda c, s: c + s.get_estimated_viewers(), streams, 0)
        clients_left_out = max(0, current_clients - total_max_clients)

        return EncoderToContentReturnStatus(clients_left_out, stream_to_content)


def get_algorithm(name: str) -> EncoderToContentAlgorithmBase:
    if name == "m-to-n":
        return MToNEncoderToContentAlgorithm()

    raise AlgorithmNotFoundError(name)


class AlgorithmNotFoundError(Exception):
    def __init__(self, name: str):
        super().__init__(f"Algorithm with name {name} not found.")
