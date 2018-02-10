# """Honeybee PointGroup and TestPointGroup."""
from __future__ import division
from ..vectormath.euclid import Point3, Vector3
from ..schedule import Schedule
from itertools import izip
import types
import copy
import ladybug.dt as dt


class GridIsNotAssigned(Exception):
    """Exception for trying to get data from and analysis point before assigning grid."""

    def __init__(self, data=None):
        data = data or 'data'
        message = '{} will only be available once AnalysisPoint ' \
            'is assigned to an AnalysisGrid.'.format(data.capitalize())

        super(GridIsNotAssigned, self).__init__(message)


class AnalysisPoint(object):
    """A radiance analysis point.

    Attributes:
        location: Location of analysis points as (x, y, z).
        direction: Direction of analysis point as (x, y, z).
    """

    __slots__ = ('_loc', '_dir', '_id_local', '_id_global', '_grid')

    def __init__(self, location, direction):
        """Create an analysis point."""
        self.location = location
        self.direction = direction
        self._grid = None  # will be assigned once added to an analysis grid
        self._id_local = None  # will be assigned once added to an analysis grid
        self._id_global = None  # will be assigned once added to a recipe

    # TODO: Add local and global id as well as grid id.
    @classmethod
    def from_json(cls, ap_json):
        """Create an analysis point from json object.
            {"location": {x: x, y: y, z: z},
            "direction": {x: x, y: y, z: z}}
        """
        location = (ap_json['location']['x'],
                    ap_json['location']['y'], ap_json['location']['z'])
        direction = (ap_json['direction']['x'],
                     ap_json['direction']['y'], ap_json['direction']['z'])

        return cls(location, direction)

    @classmethod
    def from_raw_values(cls, x, y, z, x1, y1, z1):
        """Create an analysis point from 6 values.

        x, y, z are the location of the point and x1, y1 and z1 is the direction.
        """
        return cls((x, y, z), (x1, y1, z1))

    @property
    def location(self):
        """Location of analysis points as Point3."""
        return self._loc

    @location.setter
    def location(self, location):
        try:
            self._loc = Point3(*(float(l) for l in location))
        except TypeError:
            try:
                # Dynamo Points!
                self._loc = Point3(location.X, location.Y, location.Z)
            except Exception as e:
                raise TypeError(
                    'Failed to convert {} to location.\n'
                    'location should be a list or a tuple with 3 values.\n{}'
                    .format(location, e))

    @property
    def direction(self):
        """Direction of analysis points as Point3."""
        return self._dir

    @direction.setter
    def direction(self, direction):
        try:
            self._dir = Vector3(*(float(d) for d in direction))
        except TypeError:
            try:
                # Dynamo Points!
                self._dir = Vector3(direction.X, direction.Y, direction.Z)
            except Exception as e:
                raise TypeError(
                    'Failed to convert {} to direction.\n'
                    'location should be a list or a tuple with 3 values.\n{}'
                    .format(direction, e))

    @property
    def grid(self):
        """The AnalysisGrid that this point belongs to."""
        return self._grid

    @grid.setter
    def grid(self, g):
        """Set AnalysisGrid."""
        assert hasattr('isAnalysisGrid', g), \
            'Expected AnalysisGrid not {}'.format(type(g))
        self._grid = g

    @property
    def sources(self):
        """Get sorted list of light sources."""
        if not self.grid:
            raise GridIsNotAssigned('Sources')
        return self.grid.sources

    @property
    def db_file(self):
        """Get path to database file.

        The file will be available if this point is part of an AnalysisGrid.
        """
        if not self.grid:
            raise GridIsNotAssigned('database')
        return self.grid.db_file

    @property
    def has_values(self):
        """Check if this point has results values."""
        if not self.grid:
            return False
        return self.grid.has_values

    @property
    def has_direct_values(self):
        """Check if direct values are loaded for this point.

        In some cases and based on the recipe only total values are available.
        """
        if not self.grid:
            return False
        return self.grid.has_direct_values

    @property
    def hoys(self):
        """Return hours of the year for results if any."""
        if not self.grid:
            return []
        return self.grid.hoys

    # TODO: This method needs a sample code
    @staticmethod
    def _logic(*args, **kwargs):
        """Dynamic blinds state logic.

        If the logic is not met the blind will be moved to the next state.
        Overwrite this method for optional blind control.
        """
        return args[0] > 3000

    # TODO: review if really neccessary
    def source_id(self, source):
        """Get source id from source name."""
        # find the id for source and state
        try:
            return self.sources[source]['id']
        except KeyError:
            raise ValueError('Invalid source input: {}'.format(source))

    # TODO: review if really neccessary
    def blind_state_id(self, source, state):
        """Get state id if available."""
        try:
            return int(state)
        except ValueError:
            pass

        try:
            return self.sources[source]['state'].index(state)
        except ValueError:
            raise ValueError('Invalid state input: {}'.format(state))

    # TODO: review if really neccessary
    @property
    def states(self):
        """Get list of states names for each source."""
        return tuple(s[1]['state'] for s in self.sources.iteritems())

    # TODO: review if really neccessary
    @property
    def longest_state_ids(self):
        """Get longest combination between blind states as blinds_state_ids."""
        states = tuple(len(s[1]['state']) - 1 for s in self.sources.iteritems())
        if not states:
            raise ValueError('This sensor is associated with no dynamic blinds.')

        return tuple(tuple(min(s, i) for s in states)
                     for i in range(max(states) + 1))

    # TODO: rewrite to work with AnalysisGrid
    def set_value(self, value, hoy, source=None, state=None, is_direct=False):
        """Set value for a specific hour of the year.

        Args:
            value: Value as a number.
            hoy: The hour of the year that corresponds to this value.
            source: Name of the source of light. Only needed in case of multiple
                sources / window groups (default: None).
            state: State of the source if any (default: None).
            is_direct: Set to True if the value is direct contribution of sunlight.
        """

        if hoy is None:
            return
        sid, stateid = self._create_data_structure(source, state)
        if is_direct:
            self._is_directLoaded = True
        ind = 1 if is_direct else 0
        self._values[sid][stateid][hoy][ind] = value

    # TODO: rewrite to work with AnalysisGrid
    def set_values(self, values, hoys, source=None, state=None, is_direct=False):
        """Set values for several hours of the year.

        Args:
            values: List of values as numbers.
            hoys: List of hours of the year that corresponds to input values.
            source: Name of the source of light. Only needed in case of multiple
                sources / window groups (default: None).
            state: State of the source if any (default: None).
            is_direct: Set to True if the value is direct contribution of sunlight.
        """
        if not (isinstance(values, types.GeneratorType) or
                isinstance(hoys, types.GeneratorType)):

            assert len(values) == len(hoys), \
                ValueError(
                    'Length of values [%d] is not equal to length of hoys [%d].'
                    % (len(values), len(hoys)))

        sid, stateid = self._create_data_structure(source, state)

        if is_direct:
            self._is_directLoaded = True

        ind = 1 if is_direct else 0

        for hoy, value in izip(hoys, values):
            if hoy is None:
                continue
            try:
                self._values[sid][stateid][hoy][ind] = value
            except Exception as e:
                raise ValueError(
                    'Failed to load {} results for window_group [{}], state[{}]'
                    ' for hour {}.\n{}'.format('direct' if is_direct else 'total',
                                               sid, stateid, hoy, e)
                )

    # TODO: rewrite to work with AnalysisGrid
    def set_coupled_value(self, value, hoy, source=None, state=None):
        """Set both total and direct values for a specific hour of the year.

        Args:
            value: Value as as tuples (total, direct).
            hoy: The hour of the year that corresponds to this value.
            source: Name of the source of light. Only needed in case of multiple
                sources / window groups (default: None).
            state: State of the source if any (default: None).
        """
        sid, stateid = self._create_data_structure(source, state)

        if hoy is None:
            return

        try:
            self._values[sid][stateid][hoy] = value[0], value[1]
        except TypeError:
            raise ValueError(
                "Wrong input: {}. Input values must be of length of 2.".format(value)
            )
        except IndexError:
            raise ValueError(
                "Wrong input: {}. Input values must be of length of 2.".format(value)
            )
        else:
            self._is_directLoaded = True

    # TODO: rewrite to work with AnalysisGrid
    def set_coupled_values(self, values, hoys, source=None, state=None):
        """Set total and direct values for several hours of the year.

        Args:
            values: List of values as tuples (total, direct).
            hoys: List of hours of the year that corresponds to input values.
            source: Name of the source of light. Only needed in case of multiple
                sources / window groups (default: None).
            state: State of the source if any (default: None).
        """
        if not (isinstance(values, types.GeneratorType) or
                isinstance(hoys, types.GeneratorType)):

            assert len(values) == len(hoys), \
                ValueError(
                    'Length of values [%d] is not equal to length of hoys [%d].'
                    % (len(values), len(hoys)))

        sid, stateid = self._create_data_structure(source, state)

        for hoy, value in izip(hoys, values):
            if hoy is None:
                continue
            try:
                self._values[sid][stateid][hoy] = value[0], value[1]
            except TypeError:
                raise ValueError(
                    "Wrong input: {}. Input values must be of length of 2.".format(value)
                )
            except IndexError:
                raise ValueError(
                    "Wrong input: {}. Input values must be of length of 2.".format(value)
                )
        self._is_directLoaded = True

    # TODO: rewrite to work with AnalysisGrid
    def value(self, hoy, source=None, state=None):
        """Get total value for an hour of the year."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)
        # SELECT tot FROM FinalResults WHERE sensor_id=0 AND source_id=0 AND state_id=0 AND hoy=12;
        if hoy not in self._values[sid][stateid]:
            raise ValueError('Hourly values are not available for {}.'
                             .format(dt.DateTime.fromHoy(hoy)))
        return self._values[sid][stateid][hoy][0]

    # TODO: rewrite to work with AnalysisGrid
    def direct_value(self, hoy, source=None, state=None):
        """Get direct value for an hour of the year."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)
        # SELECT sun FROM FinalResults WHERE sensor_id=0 AND source_id=0 AND state_id=0 AND hoy=12;
        if hoy not in self._values[sid][stateid]:
            raise ValueError('Hourly values are not available for {}.'
                             .format(dt.DateTime.fromHoy(hoy)))
        return self._values[sid][stateid][hoy][1]

    # TODO: rewrite to work with AnalysisGrid
    def values(self, hoys=None, source=None, state=None):
        """Get values for several hours of the year."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)

        hoys = hoys or self.hoys
        # generate the tuple for cases and executemany
        # SELECT tot FROM FinalResults WHERE sensor_id=0 AND source_id=0 AND state_id=0 AND hoy=12;
        for hoy in hoys:
            if hoy not in self._values[sid][stateid]:
                raise ValueError('Hourly values are not available for {}.'
                                 .format(dt.DateTime.fromHoy(hoy)))

        return tuple(self._values[sid][stateid][hoy][0] for hoy in hoys)

    # TODO: rewrite to work with AnalysisGrid
    def direct_values(self, hoys=None, source=None, state=None):
        """Get direct values for several hours of the year."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)
        hoys = hoys or self.hoys
        # SELECT sun FROM FinalResults WHERE sensor_id=0 AND source_id=0 AND state_id=0 AND hoy IN ({});

        for hoy in hoys:
            if hoy not in self._values[sid][stateid]:
                raise ValueError('Hourly values are not available for {}.'
                                 .format(dt.DateTime.fromHoy(hoy)))
        return tuple(self._values[sid][stateid][hoy][1] for hoy in hoys)

    # TODO: rewrite to work with AnalysisGrid
    def coupled_value(self, hoy, source=None, state=None):
        """Get total and direct values for an hoy."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)
        # SELECT tot, sun FROM FinalResults WHERE sensor_id=0 AND source_id=0 AND state_id=0 AND hoy=12;

        if hoy not in self._values[sid][stateid]:
            raise ValueError('Hourly values are not available for {}.'
                             .format(dt.DateTime.fromHoy(hoy)))
        return self._values[sid][stateid][hoy]

    # TODO: rewrite to work with AnalysisGrid
    def coupled_values(self, hoys=None, source=None, state=None):
        """Get total and direct values for several hours of year."""
        # find the id for source and state
        sid = self.source_id(source)
        # find the state id
        stateid = self.blind_state_id(source, state)

        hoys = hoys or self.hoys

        for hoy in hoys:
            if hoy not in self._values[sid][stateid]:
                raise ValueError('Hourly values are not available for {}.'
                                 .format(dt.DateTime.fromHoy(hoy)))

        return tuple(self._values[sid][stateid][hoy] for hoy in hoys)

    # TODO: rewrite to work with AnalysisGrid
    def coupled_value_by_id(self, hoy, source_id=None, state_id=None):
        """Get total and direct values for an hoy."""
        # find the id for source and state
        sid = source_id or 0
        # find the state id
        stateid = state_id or 0

        if hoy not in self._values[sid][stateid]:
            raise ValueError('Hourly values are not available for {}.'
                             .format(dt.DateTime.fromHoy(hoy)))

        return self._values[sid][stateid][hoy]

    # TODO: rewrite to work with AnalysisGrid
    def coupled_values_by_id(self, hoys=None, source_id=None, state_id=None):
        """Get total and direct values for several hours of year by source id.

        Use this method to load the values if you have the ids for source and state.

        Args:
            hoys: A collection of hoys.
            source_id: Id of source as an integer (default: 0).
            state_id: Id of state as an integer (default: 0).
        """
        sid = source_id or 0
        stateid = state_id or 0

        hoys = hoys or self.hoys

        for hoy in hoys:
            if hoy not in self._values[sid][stateid]:
                raise ValueError('Hourly values are not available for {}.'
                                 .format(dt.DateTime.fromHoy(hoy)))

        return tuple(self._values[sid][stateid][hoy] for hoy in hoys)

    # TODO: rewrite to work with AnalysisGrid
    def combined_value_by_id(self, hoy, blinds_state_ids=None):
        """Get combined value from all sources based on state_id.

        Args:
            hoy: hour of the year.
            blinds_state_ids: List of state ids for all the sources for an hour. If you
                want a source to be removed set the state to -1.

        Returns:
            total, direct values.
        """
        total = 0
        direct = 0 if self._is_directLoaded else None

        if not blinds_state_ids:
            blinds_state_ids = [0] * len(self.sources)

        assert len(self.sources) == len(blinds_state_ids), \
            'There should be a state for each source. #sources[{}] != #states[{}]' \
            .format(len(self.sources), len(blinds_state_ids))

        for sid, stateid in enumerate(blinds_state_ids):

            if stateid == -1:
                t = 0
                d = 0
            else:
                if hoy not in self._values[sid][stateid]:
                    raise ValueError('Hourly values are not available for {}.'
                                     .format(dt.DateTime.fromHoy(hoy)))
                t, d = self._values[sid][stateid][hoy]

            try:
                total += t
                direct += d
            except TypeError:
                # direct value is None
                pass

        return total, direct

    # TODO: rewrite to work with AnalysisGrid
    def combined_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get combined value from all sources based on state_id.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a generator for (total, direct) values.
        """
        hoys = hoys or self.hoys

        if not blinds_state_ids:
            try:
                hours_count = len(hoys)
            except TypeError:
                raise TypeError('hoys must be an iterable object: {}'.format(hoys))
            blinds_state_ids = [[0] * len(self.sources)] * hours_count

        assert len(hoys) == len(blinds_state_ids), \
            'There should be a list of states for each hour. #states[{}] != #hours[{}]' \
            .format(len(blinds_state_ids), len(hoys))

        dir_value = 0 if self._is_directLoaded else None
        for count, hoy in enumerate(hoys):
            total = 0
            direct = dir_value

            for sid, stateid in enumerate(blinds_state_ids[count]):
                if stateid == -1:
                    t = 0
                    d = 0
                else:
                    if hoy not in self._values[sid][stateid]:
                        raise ValueError('Hourly values are not available for {}.'
                                         .format(dt.DateTime.fromHoy(hoy)))
                    t, d = self._values[sid][stateid][hoy]

                try:
                    total += t
                    direct += d
                except TypeError:
                    # direct value is None
                    pass

            yield total, direct

    # TODO: rewrite to work with AnalysisGrid
    def sum_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get sum of value for all the hours.

        This method is mostly useful for radiation and solar access analysis.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a tuple for sum of (total, direct) values.
        """
        values = tuple(self.combined_values_by_id(hoys, blinds_state_ids))

        total = sum(v[0] for v in values)
        try:
            direct = sum(v[1] for v in values)
        except TypeError as e:
            if "'long' and 'NoneType'" in str(e):
                # direct value is not loaded
                direct = 0
            else:
                raise TypeError(e)

        return total, direct

    # TODO: rewrite to work with AnalysisGrid
    def max_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get maximum value for all the hours.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a tuple for max of (total, direct) values.
        """
        values = tuple(self.combined_values_by_id(hoys, blinds_state_ids))

        total = max(v[0] for v in values)
        direct = max(v[1] for v in values)

        return total, direct

    # TODO: rewrite to work with AnalysisGrid
    def blinds_state(self, hoys=None, blinds_state_ids=None, *args, **kwargs):
        """Calculte blinds state based on a control logic.

        Overwrite self.logic to overwrite the logic for this point.

        Args:
            hoys: List of hours of year. If None default is self.hoys.
            blinds_state_ids: List of state ids for all the sources for an hour. If you
                want a source to be removed set the state to -1. If not provided
                a longest combination of states from sources (window groups) will
                be used. Length of each item in states should be equal to number
                of sources.
            args: Additional inputs for self.logic. args will be passed to self.logic
            kwargs: Additional inputs for self.logic. kwargs will be passed to self.logic
        """
        hoys = hoys or self.hoys

        if blinds_state_ids:
            # recreate the states in case the inputs are the names of the states
            # and not the numbers.
            sources = self.sources

            comb_ids = copy.deepcopy(blinds_state_ids)

            # find state ids for each state if inputs are state names
            try:
                for c, comb in enumerate(comb_ids):
                    for count, source in enumerate(sources):
                        comb_ids[c][count] = self.blind_state_id(source, comb[count])
            except IndexError:
                raise ValueError(
                    'Length of each state should be equal to number of sources: {}'
                    .format(len(sources))
                )
        else:
            comb_ids = self.longest_state_ids

        print("Blinds combinations:\n{}".format(
              '\n'.join(str(ids) for ids in comb_ids)))

        # collect the results for each combination
        results = range(len(comb_ids))
        for count, state in enumerate(comb_ids):
            results[count] = tuple(self.combined_values_by_id(hoys, [state] * len(hoys)))

        # assume the last state happens for all
        hours_count = len(hoys)
        blinds_index = [len(comb_ids) - 1] * hours_count
        ill_values = [None] * hours_count
        dir_values = [None] * hours_count
        success = [0] * hours_count

        for count, h in enumerate(hoys):
            for state in range(len(comb_ids)):
                ill, ill_dir = results[state][count]
                if not self.logic(ill, ill_dir, h, args, kwargs):
                    blinds_index[count] = state
                    ill_values[count] = ill
                    dir_values[count] = ill_dir
                    if state > 0:
                        success[count] = 1
                    break
            else:
                success[count] = -1
                ill_values[count] = ill
                dir_values[count] = ill_dir

        blinds_state = tuple(comb_ids[ids] for ids in blinds_index)
        return blinds_state, blinds_index, ill_values, dir_values, success

    # TODO: rewrite to work with AnalysisGrid
    def annual_metrics(self, da_threshhold=None, udi_min_max=None, blinds_state_ids=None,
                       occ_schedule=None):
        """Calculate annual metrics.

        Daylight autonomy, continious daylight autonomy and useful daylight illuminance.

        Args:
            da_threshhold: Threshhold for daylight autonomy in lux (default: 300).
            udi_min_max: A tuple of min, max value for useful daylight illuminance
                (default: (100, 2000)).
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule (default: Office Schedule).

        Returns:
            Daylight autonomy, Continious daylight autonomy, Useful daylight illuminance,
            Less than UDI, More than UDI
        """
        hours = self.hoys
        values = tuple(v[0] for v in self.combined_values_by_id(hours, blinds_state_ids))

        return self._calculate_annual_metrics(
            values, hours, da_threshhold, udi_min_max, blinds_state_ids, occ_schedule)

    # TODO: rewrite to work with AnalysisGrid
    def useful_daylight_illuminance(self, udi_min_max=None, blinds_state_ids=None,
                                    occ_schedule=None):
        """Calculate useful daylight illuminance.

        Args:
            udi_min_max: A tuple of min, max value for useful daylight illuminance
                (default: (100, 2000)).
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule.

        Returns:
            Useful daylight illuminance, Less than UDI, More than UDI
        """
        udi_min_max = udi_min_max or (100, 2000)
        udiMin, udiMax = udi_min_max
        hours = self.hoys
        schedule = occ_schedule or set(hours)
        udi = 0
        udi_l = 0
        udi_m = 0
        total_hour_count = len(hours)
        values = tuple(v[0] for v in self.combined_values_by_id(hours, blinds_state_ids))
        for h, v in izip(hours, values):
            if h not in schedule:
                total_hour_count -= 1
                continue
            if v < udiMin:
                udi_l += 1
            elif v > udiMax:
                udi_m += 1
            else:
                udi += 1

        if total_hour_count == 0:
            raise ValueError('There is 0 hours available in the schedule.')

        return 100 * udi / total_hour_count, 100 * udi_l / total_hour_count, \
            100 * udi_m / total_hour_count

    # TODO: rewrite to work with AnalysisGrid
    def daylight_autonomy(self, da_threshhold=None, blinds_state_ids=None,
                          occ_schedule=None):
        """Calculate daylight autonomy and continious daylight autonomy.

        Args:
            da_threshhold: Threshhold for daylight autonomy in lux (default: 300).
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule.

        Returns:
            Daylight autonomy, Continious daylight autonomy
        """
        da_threshhold = da_threshhold or 300
        hours = self.hoys
        schedule = occ_schedule or set(hours)
        DA = 0
        cda = 0
        total_hour_count = len(hours)
        values = tuple(v[0] for v in self.combined_values_by_id(hours, blinds_state_ids))
        for h, v in izip(hours, values):
            if h not in schedule:
                total_hour_count -= 1
                continue
            if v >= da_threshhold:
                DA += 1
                cda += 1
            else:
                cda += v / da_threshhold

        if total_hour_count == 0:
            raise ValueError('There is 0 hours available in the schedule.')

        return 100 * DA / total_hour_count, 100 * cda / total_hour_count

    # TODO: rewrite to work with AnalysisGrid
    def annual_solar_exposure(self, threshhold=None, blinds_state_ids=None,
                              occ_schedule=None, target_hours=None):
        """Annual Solar Exposure (ASE).

        Calculate number of hours that this point is exposed to more than 1000lux
        of direct sunlight. The point meets the traget in the number of hours is
        less than 250 hours per year.

        Args:
            threshhold: Threshhold for daylight autonomy in lux (default: 1000).
            blinds_state_ids: List of state ids for all the sources for input hoys.
                If you want a source to be removed set the state to -1. ase must
                be calculated without dynamic blinds but you can use this option
                to study the effect of different blind states.
            occ_schedule: An annual occupancy schedule.
            target_hours: Target minimum hours (default: 250).

        Returns:
            Success as a Boolean, Number of hours, Problematic hours
        """
        if not self.has_direct_values:
            raise ValueError(
                'Direct values are not loaded. Data is not available to calculate ASE.')

        hoys = self.hoys
        values = tuple(v[1] for v in self.combined_values_by_id(hoys, blinds_state_ids))
        return self._calculate_annual_solar_exposure(
            values, hoys, threshhold, blinds_state_ids, occ_schedule, target_hours)

    # TODO: rewrite to work with AnalysisGrid
    @staticmethod
    def _calculate_annual_solar_exposure(
            values, hoys, threshhold=None, blinds_state_ids=None, occ_schedule=None,
            target_hours=None):
        threshhold = threshhold or 1000
        target_hours = target_hours or 250
        schedule = occ_schedule or set(hoys)
        ase = 0
        problematic_hours = []
        for h, v in izip(hoys, values):
            if h not in schedule:
                continue
            if v > threshhold:
                ase += 1
                problematic_hours.append(h)

        return ase < target_hours, ase, problematic_hours

    # TODO: rewrite to work with AnalysisGrid
    @staticmethod
    def _calculate_annual_metrics(
        values, hours, da_threshhold=None, udi_min_max=None, blinds_state_ids=None,
            occ_schedule=None):
        total_hour_count = len(hours)
        udiMin, udiMax = udi_min_max
        udi_min_max = udi_min_max or (100, 2000)
        da_threshhold = da_threshhold or 300.0
        schedule = occ_schedule or Schedule.from_workday_hours()
        DA = 0
        cda = 0
        udi = 0
        udi_l = 0
        udi_m = 0
        for h, v in izip(hours, values):
            if h not in schedule:
                total_hour_count -= 1
                continue
            if v >= da_threshhold:
                DA += 1
                cda += 1
            else:
                cda += v / da_threshhold

            if v < udiMin:
                udi_l += 1
            elif v > udiMax:
                udi_m += 1
            else:
                udi += 1

        if total_hour_count == 0:
            raise ValueError('There is 0 hours available in the schedule.')

        return 100 * DA / total_hour_count, 100 * cda / total_hour_count, \
            100 * udi / total_hour_count, 100 * udi_l / total_hour_count, \
            100 * udi_m / total_hour_count

    # TODO: rewrite to work with AnalysisGrid
    @staticmethod
    def _calculate_daylight_autonomy(
            values, hoys, da_threshhold=None, blinds_state_ids=None, occ_schedule=None):
        """Calculate daylight autonomy and continious daylight autonomy.

        Args:
            da_threshhold: Threshhold for daylight autonomy in lux (default: 300).
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule.

        Returns:
            Daylight autonomy, Continious daylight autonomy
        """
        da_threshhold = da_threshhold or 300
        hours = hoys
        schedule = occ_schedule or set(hours)
        DA = 0
        cda = 0
        total_hour_count = len(hours)
        for h, v in izip(hours, values):
            if h not in schedule:
                total_hour_count -= 1
                continue
            if v >= da_threshhold:
                DA += 1
                cda += 1
            else:
                cda += v / da_threshhold

        if total_hour_count == 0:
            raise ValueError('There is 0 hours available in the schedule.')

        return 100 * DA / total_hour_count, 100 * cda / total_hour_count

    @staticmethod
    def parse_blind_states(blinds_state_ids):
        """Parse input blind states.

        The method tries to convert each state to a tuple of a list. Use this method
        to parse the input from plugins.

        Args:
            blinds_state_ids: List of state ids for all the sources for an hour. If you
                want a source to be removed set the state to -1. If not provided
                a longest combination of states from sources (window groups) will
                be used. Length of each item in states should be equal to number
                of sources.
        """
        try:
            combs = [list(eval(cc)) for cc in blinds_state_ids]
        except Exception as e:
            ValueError('Failed to convert input blind states:\n{}'.format(e))

        return combs

    # TODO: include ids
    def duplicate(self):
        """Duplicate the analysis point."""
        ap = AnalysisPoint(self._loc, self._dir)
        ap.logic = copy.copy(self.logic)
        return ap

    def ToString(self):
        """Overwrite .NET ToString."""
        return self.__repr__()

    def to_rad_string(self):
        """Return Radiance string for a test point."""
        return "%s %s" % (self.location, self.direction)

    # TODO: include ids
    def to_json(self):
        """Create an analysis point from json object.
            {"location": {x: x, y: y, z: z}, "direction": {x: x, y: y, z: z}}
        """
        return {'location': {'x': self.location[0], 'y': self.location[1],
                             'z': self.location[2]},
                'direction': {'x': self.direction[0], 'y': self.direction[1],
                              'z': self.direction[2]}
                }

    def __repr__(self):
        """Print an analysis point."""
        return 'AnalysisPoint::(%s)::(%s)' % (self.location, self.direction)
