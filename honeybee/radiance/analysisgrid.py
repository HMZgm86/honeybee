"""Honeybee PointGroup and TestPointGroup."""
from __future__ import division
from ..utilcol import random_name
from ..dataoperation import match_data
from .analysispoint import AnalysisPoint

import os
from itertools import izip
from collections import OrderedDict


class AnalysisGrid(object):
    """A grid of analysis points.

    Attributes:
        analysis_points: A collection of analysis points.
    """

    __slots__ = ('_analysis_points', '_name', '_luminous_sources', '_anlysis_result',
                 '_id_local', '_id_global')

    def __init__(self, analysis_points, name=None, luminous_sources=None):
        """Initialize a AnalysisPointGroup.

        Args:
            analysis_points: A collection of AnalysisPoints.
            name: A unique name for this AnalysisGrid.
            luminous_sources: A collection of luminous sources associated to this
                analysis grid. In case it is left empty all the luminous sources will
                be assigned to this analysis grid once it is added to a recipe.
                In multi-phase daylight simulations luminous sources are window groups
                most of time. In 2-phase recipe the luminous source is sky.
        """
        self.name = name
        self.luminous_sources = luminous_sources
        self.analysis_points = analysis_points
        self._anlysis_result = None  # will be assigned once added to a recipe
        self._id_local = None  # will be set once added to an analysis recipe
        self._id_global = None  # will be assigned once added to a database

    # TODO: rewrite this method once the class is updated.
    @classmethod
    def from_json(cls, ag_json):
        """Create an analysis grid from json objects."""
        analysis_points = tuple(AnalysisPoint.from_json(pt)
                                for pt in ag_json["analysis_points"])
        return cls(analysis_points)

    @classmethod
    def from_points_and_vectors(cls, points, vectors=None,
                                name=None, luminous_sources=None):
        """Create an analysis grid from points and vectors.

        Args:
            points: A flatten list of (x, y ,z) points.
            vectors: An optional list of (x, y, z) for direction of test points.
                If not provided a (0, 0, 1) vector will be assigned.
            name: A unique name for this AnalysisGrid.
            luminous_sources: A collection of luminous sources associated to this
                analysis grid. In case it is left empty all the luminous sources will
                be assigned to this analysis grid once it is added to a recipe.
                In multi-phase daylight simulations luminous sources are window groups
                most of time. In 2-phase recipe the luminous source is sky.
        """
        vectors = vectors or ()
        points, vectors = match_data(points, vectors, (0, 0, 1))
        aps = tuple(AnalysisPoint(pt, v) for pt, v in izip(points, vectors))
        return cls(aps, name, luminous_sources)

    @classmethod
    def from_file(cls, file_path, name=None, luminous_sources=None):
        """Create an analysis grid from a pts file.

        The file should have 6 values in each line indicating location and direction
        of each test point.

        Args:
            file_path: Full path to points file.
            name: A unique name for this AnalysisGrid.
            luminous_sources: A collection of luminous sources associated to this
                analysis grid. In case it is left empty all the luminous sources will
                be assigned to this analysis grid once it is added to a recipe.
                In multi-phase daylight simulations luminous sources are window groups
                most of time. In 2-phase recipe the luminous source is sky.
        """
        assert os.path.isfile(file_path), IOError("Can't find {}.".format(file_path))
        ap = AnalysisPoint  # load analysis point locally for better performance
        with open(file_path, 'rb') as inf:
            points = tuple(ap.from_raw_values(*l.split()) for l in inf)
        if not name:
            name = os.path.split(file_path)[-1][:-4]
        return cls(points, name, luminous_sources)

    @property
    def isAnalysisGrid(self):
        """Return True for AnalysisGrid."""
        return True

    @property
    def name(self):
        """AnalysisGrid name."""
        return self._name

    @name.setter
    def name(self, n):
        self._name = n or random_name()

    @property
    def luminous_sources(self):
        """A list of window group names that are related to this analysis grid."""
        return self._luminous_sources

    # TODO: up
    @luminous_sources.setter
    def luminous_sources(self, wgs):
        if not wgs:
            wgs = ()
        for wg in wgs:
            assert hasattr(wg, 'isWindowGroup'), \
                'Expected WindowGroup not {}.'.format(type(wg))
        self._luminous_sources = wgs

    @property
    def analysis_points(self):
        """Return a list of analysis points."""
        return self._analysis_points

    @analysis_points.setter
    def analysis_points(self, aps):
        """Set list of analysis points."""
        for count, ap in enumerate(aps):
            assert hasattr(ap, '_loc'), \
                '{} is not an AnalysisPoint.'.format(type(ap))
            ap.grid = self  # set grid for analysis point
            ap.id = count

    @property
    def locations(self):
        """A generator of analysis points locations as x, y, z."""
        return (ap.location for ap in self._analysis_points)

    @property
    def vectors(self):
        """Get generator of analysis points directions as x, y , z."""
        return (ap.direction for ap in self._analysis_points)

    @property
    def result(self):
        """Return AnalysisResult for this grid."""
        return None

    @result.setter
    def result(self, res):
        """Set Analysis Result for this grid."""
        pass

    def renumber(self, start_from):
        """Renumber global id for analysis points in this grid.

        Args:
            start_from: An integer id for the first point in this AnalysisGrid.

        Returns:
            An integer which indicates the id for the last point in this AnalysisGrid.
        """
        for count, point in enumerate(self.analysis_points):
            point.id = start_from + count
        print('Renumbered points from {} to {}'.format(start_from, start_from + count))
        return start_from + count

    # TODO: rewrite once updating the class is finished.
    def unload(self):
        """Remove all the sources and values from analysis_points."""
        self._totalFiles = []
        self._directFiles = []

        for ap in self._analysis_points:
            ap._sources = OrderedDict()
            ap._values = []

    def duplicate(self):
        """Duplicate AnalysisGrid."""
        aps = tuple(ap.duplicate() for ap in self._analysis_points)
        dup = AnalysisGrid(aps, self._name)
        dup._luminous_sources = self._luminous_sources
        return dup

    def to_rad_string(self):
        """Return analysis points group as a Radiance string."""
        return "\n".join((ap.to_rad_string() for ap in self._analysis_points))

    def ToString(self):
        """Overwrite ToString .NET method."""
        return self.__repr__()

    def to_json(self):
        """Create an analysis grid from json objects."""
        analysis_points = [ap.to_json() for ap in self.analysis_points]
        return {"analysis_points": analysis_points}

    def __add__(self, other):
        """Add two analysis grids and create a new one.

        This method won't duplicate the analysis points.
        """
        assert isinstance(other, AnalysisGrid), \
            TypeError('Expected an AnalysisGrid not {}.'.format(type(other)))

        assert self.hoys == other.hoys, \
            ValueError('Two analysis grid must have the same hoys.')

        if not self.has_values:
            sources = self._luminous_sources.update(other._sources)
        else:
            assert self._luminous_sources == other._sources, \
                ValueError(
                    'Two analysis grid with values must have the same luminous_sources.'
                )
            sources = self._luminous_sources

        points = self.analysis_points + other.analysis_points
        name = '{}+{}'.format(self.name, other.name)
        addition = AnalysisGrid(points, name)
        addition._luminous_sources = sources

        return addition

    def __len__(self):
        """Number of points in this group."""
        return len(self._analysis_points)

    def __getitem__(self, index):
        """Get value for an index."""
        return self._analysis_points[index]

    def __iter__(self):
        """Iterate points."""
        return iter(self._analysis_points)

    def __str__(self):
        """String repr."""
        return self.to_rad_string()

    @property
    def digit_sign(self):
        if not self.has_values:
            if len(self.result_files[0]) + len(self.result_files[1]) == 0:
                # only x, y, z datat is available
                return 0
            else:
                # results are available but are not loaded yet
                return 1
        elif self.is_results_point_in_time:
            # results is loaded for a single hour
            return 2
        else:
            # results is loaded for multiple hours
            return 3

    @property
    def _sign(self):
        if not self.has_values:
            if len(self.result_files[0]) + len(self.result_files[1]) == 0:
                # only x, y, z datat is available
                return '[.]'
            else:
                # results are available but are not loaded yet
                return '[/]'
        elif self.is_results_point_in_time:
            # results is loaded for a single hour
            return '[+]'
        else:
            # results is loaded for multiple hours
            return '[*]'

    def __repr__(self):
        """Return analysis points and directions."""
        return 'AnalysisGrid::{}::#{}::{}'.format(
            self._name, len(self._analysis_points), self._sign
        )
