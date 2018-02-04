"""Base class for GridResults."""
from ..analysisgrid import AnalysisGrid


class GridResults(AnalysisGrid):
    """Base class for GridResults."""

    def __init__(self, analysis_points, name=None, luminous_groups=None):
        super(GridResults, self).__init__(analysis_points, name=None, window_groups=None)
        self._database = None

    @classmethod
    def from_analysis_grid(cls, analysis_grid):
        pass

    def set_values(self):
        pass
