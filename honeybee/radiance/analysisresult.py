"""AnalysisResult to interact with results of a grid-based study.

For image-based studies see image collection.
"""
from database import SqliteDB
from ..exception import GridIsNotAssignedError, NoDirectValueError
from ..schedule import Schedule
from ..exception import EmptyFileError


class AnalysisResult(object):
    """Analysis result for Radiance's grid-based studies.

    This class is read-only and is used to connect to a local sqlite3 database.

    Args:
        db_file: Path to local database file.

    """

    def __init__(self, db_file):
        self._database = db_file
        self._grid = None  # will be assigned once added to an analysis grid

    @property
    def is_point_in_time(self):
        pass

    @property
    def is_annual(self):
        pass

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
        pass

    @property
    def hoys(self):
        """Return hours of the year for results if any."""
        pass

    @property
    def progress(self):
        """Number of results available.

        This is useful to query analysis progress.
        """
        pass

    def value(self, hoy, source=None, state=None, sensor_id=None):
        """Get total hourly value for a single hour of the year.

        Total value is the addition of direct and diffuse/sky values.

        Args:
            hoy: A single hour of the year.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of values sorted based on sensor id.
        """
        pass

    def value_direct(self, hoy, source=None, state=None, sensor_id=None):
        """Get direct hourly value from sun for a single hour of the year.

        Args:
            hoy: A single hour of the year.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of values sorted based on sensor id.

        Exceptions:
            NoDirectValueError: This exception will raise when the analysis doesn't
                generate direct values separately. Daylight factor, Solar access,
                Piont-in-time and 3-phase recipes are the recipes which don't calculate
                the direct values separately.
        """
        pass

    def values(self, hoys=None, source=None, state=None, sensor_id=None):
        """Get total hourly value for several hours of the year.

        Total value is the addition of direct and diffuse/sky values.

        Args:
            hoys: A list of hours. If not provided it will be set to self.hoys.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of list of values sorted based on sensor id for each sensor.
        """
        pass

    def values_direct(self, hoys=None, source=None, state=None, sensor_id=None):
        """Get direct hourly value from sun for a single hour of the year.

        Args:
            hoy: A single hour of the year.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of values sorted based on sensor id.

        Exceptions:
            NoDirectValueError: This exception will raise when the analysis doesn't
                generate direct values separately. Daylight factor, Solar access,
                Piont-in-time and 3-phase recipes are the recipes which don't calculate
                the direct values separately.
        """
        pass

    def coupled_value(self, hoy, source=None, state=None, sensor_id=None):
        """Get total hourly value for a single hour of the year.

        Total value is the addition of direct and diffuse/sky values.

        Args:
            hoy: A single hour of the year.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of values sorted based on sensor id.
        """
        pass

    def coupled_values(self, hoys=None, source=None, state=None, sensor_id=None):
        """Get direct hourly value from sun for a single hour of the year.

        Args:
            hoy: A single hour of the year.
            source: Name of a luminious source (default: sky).
            state: Name of a luminious source state (default: init).
            sensor_id: Optional sensor id for a specific sensor. If None this method
                returns the value for all the available sensors.

        Returns:
            A list of values sorted based on sensor id.

        Exceptions:
            NoDirectValueError: This exception will raise when the analysis doesn't
                generate direct values separately. Daylight factor, Solar access,
                Piont-in-time and 3-phase recipes are the recipes which don't calculate
                the direct values separately.
        """
        pass

    def combined_value_by_id(self, hoy, blinds_state_ids=None):
        """Get combined value from all sources based on state_id.

        Args:
            hoy: hour of the year.
            blinds_state_ids: List of state ids for all the sources for an hour. If you
                want a source to be removed set the state to -1.

        Returns:
            total, direct values.
        """
        if self.digit_sign == 1:
            self.load_values_from_files()

        return (p.combined_value_by_id(hoy, blinds_state_ids) for p in self)

    def combined_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get combined value from all sources based on state_ids.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a generator for (total, direct) values.
        """
        if self.digit_sign == 1:
            self.load_values_from_files()

        return (p.combined_value_by_id(hoys, blinds_state_ids) for p in self)

    def sum_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get sum of value for all the hours.

        This method is mostly useful for radiation and solar access analysis.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a collection of sum values as (total, direct) values.
        """
        if self.digit_sign == 1:
            self.load_values_from_files()

        return (p.sum_values_by_id(hoys, blinds_state_ids) for p in self)

    def max_values_by_id(self, hoys=None, blinds_state_ids=None):
        """Get maximum value for all the hours.

        Args:
            hoys: A collection of hours of the year.
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.

        Returns:
            Return a tuple for sum of (total, direct) values.
        """
        if self.digit_sign == 1:
            self.load_values_from_files()

        return (p.max_values_by_id(hoys, blinds_state_ids) for p in self)

    def annual_metrics(self, da_threshhold=None, udi_min_max=None, blinds_state_ids=None,
                       occ_schedule=None):
        """Calculate annual metrics.

        Daylight autonomy, continious daylight autonomy and useful daylight illuminance.

        Args:
            da_threshhold: Threshhold for daylight autonomy in lux (default: 300).
            udi_min_max: A tuple of min, max value for useful daylight illuminance
                (default: (100, 3000)).
            blinds_state_ids: List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule.

        Returns:
            Daylight autonomy, Continious daylight autonomy, Useful daylight illuminance,
            Less than UDI, More than UDI
        """
        results_loaded = True
        if not self.has_values and not self.result_files[0]:
            raise ValueError('No values are assigned to this analysis grid.')
        elif not self.has_values:
            # results are not loaded but are available
            assert len(self.result_files[0]) == 1, \
                ValueError(
                    'Annual recipe can currently only handle '
                    'a single merged result file.'
            )
            results_loaded = False
            print('Loading the results from result files.')

        res = ([], [], [], [], [])

        da_threshhold = da_threshhold or 300.0
        udi_min_max = udi_min_max or (100, 3000)
        hoys = self.hoys
        occ_schedule = occ_schedule or Schedule.from_workday_hours()

        if results_loaded:
            blinds_state_ids = blinds_state_ids or [[0] * len(self.sources)] * len(hoys)

            for sensor in self.analysis_points:
                for c, r in enumerate(sensor.annual_metrics(da_threshhold,
                                                            udi_min_max,
                                                            blinds_state_ids,
                                                            occ_schedule
                                                            )):
                    res[c].append(r)
        else:
            # This is a method for annual recipe to load the results line by line
            # which unlike the other method doesn't load all the values to the memory
            # at once.
            blinds_state_ids = [[0] * len(self.sources)] * len(hoys)
            calculate_annual_metrics = self.analysis_points[0]._calculate_annual_metrics

            for file_data in self.result_files[0]:
                file_path, hoys, start_line, header, mode = file_data

                # read the results line by line and caluclate the values
                if os.path.getsize(file_path) < 2:
                    raise EmptyFileError(file_path)

                assert mode == 0, \
                    TypeError(
                        'Annual results can only be calculated from '
                        'illuminance studies.')

                st = start_line or 0

                with open(file_path, 'rb') as inf:
                    if header:
                        inf, _ = self.parse_header(inf, st, hoys, False)

                    for i in xrange(st):
                        inf.next()

                    end = len(self._analysis_points)

                    # load one line at a time
                    for count in xrange(end):
                        values = (int(float(r)) for r in inf.next().split())
                        for c, r in enumerate(
                            calculate_annual_metrics(
                                values, hoys, da_threshhold, udi_min_max,
                                blinds_state_ids, occ_schedule)):

                            res[c].append(r)

        return res

    def spatial_daylight_autonomy(self, da_threshhold=None, target_da=None,
                                  blinds_state_ids=None, occ_schedule=None):
        """Calculate Spatial Daylight Autonomy (sDA).

        Args:
            da_threshhold: Minimum illuminance threshhold for daylight (default: 300).
            target_da: Minimum threshhold for daylight autonomy in percentage
                (default: 50%).
            blinds_state_ids:  List of state ids for all the sources for input hoys. If
                you want a source to be removed set the state to -1.
            occ_schedule: An annual occupancy schedule.

        Returns:
            sDA: Spatial daylight autonomy as percentage of analysis points.
            DA: Daylight autonomy for each analysis point.
            Problematic points: List of problematic points.
        """
        results_loaded = True
        if not self.has_values and not self.result_files[0]:
            raise ValueError('No values are assigned to this analysis grid.')
        elif not self.has_values:
            # results are not loaded but are available
            assert len(self.result_files[0]) == 1, \
                ValueError(
                    'Annual recipe can currently only handle '
                    'a single merged result file.'
            )
            results_loaded = False
            print('Loading the results from result files.')

        res = ([], [])

        da_threshhold = da_threshhold or 300.0
        target_da = target_da or 50.0
        hoys = self.hoys
        occ_schedule = occ_schedule or Schedule.from_workday_hours()

        if results_loaded:
            blinds_state_ids = blinds_state_ids or [[0] * len(self.sources)] * len(hoys)

            for sensor in self.analysis_points:
                for c, r in enumerate(sensor.daylight_autonomy(da_threshhold,
                                                               blinds_state_ids,
                                                               occ_schedule
                                                               )):
                    res[c].append(r)
        else:
            # This is a method for annual recipe to load the results line by line
            # which unlike the other method doesn't load all the values to the memory
            # at once.
            blinds_state_ids = [[0] * len(self.sources)] * len(hoys)
            calculate_daylight_autonomy = \
                self.analysis_points[0]._calculate_daylight_autonomy

            for file_data in self.result_files[0]:
                file_path, hoys, start_line, header, mode = file_data

                # read the results line by line and caluclate the values
                if os.path.getsize(file_path) < 2:
                    raise EmptyFileError(file_path)

                assert mode == 0, \
                    TypeError(
                        'Annual results can only be calculated from '
                        'illuminance studies.')

                st = start_line or 0

                with open(file_path, 'rb') as inf:
                    if header:
                        inf, _ = self.parse_header(inf, st, hoys, False)

                    for i in xrange(st):
                        inf.next()

                    end = len(self._analysis_points)

                    # load one line at a time
                    for count in xrange(end):
                        values = (int(float(r)) for r in inf.next().split())
                        for c, r in enumerate(
                            calculate_daylight_autonomy(
                                values, hoys, da_threshhold,
                                blinds_state_ids, occ_schedule)):

                            res[c].append(r)

        daylight_autonomy = res[0]
        problematic_points = []
        for pt, da in izip(self.analysis_points, daylight_autonomy):
            if da < target_da:
                problematic_points.append(pt)
        try:
            sda = (1 - len(problematic_points) / len(self.analysis_points)) * 100
        except ZeroDivisionError:
            sda = 0

        return sda, daylight_autonomy, problematic_points

    def annual_solar_exposure(self, threshhold=None, blinds_state_ids=None,
                              occ_schedule=None, target_hours=None, target_area=None):
        """Annual Solar Exposure (ASE)

        As per IES-LM-83-12 ase is the percent of sensors that are
        found to be exposed to more than 1000lux of direct sunlight for
        more than 250hrs per year. For LEED credits No more than 10% of
        the points in the grid should fail this measure.

        Args:
            threshhold: Threshhold for for solar exposure in lux (default: 1000).
            blinds_state_ids: List of state ids for all the sources for input hoys.
                If you want a source to be removed set the state to -1. ase must
                be calculated without dynamic blinds but you can use this option
                to study the effect of different blind states.
            occ_schedule: An annual occupancy schedule.
            target_hours: Minimum targe hours for each point (default: 250).
            target_area: Minimum target area percentage for this grid (default: 10).

        Returns:
            Success as a Boolean, ase values for each point, Percentage area,
            Problematic points, Problematic hours for each point
        """
        results_loaded = True
        if not self.has_direct_values and not self.result_files[1]:
            raise ValueError(
                'Direct values are not available to calculate ASE.\nIn most of the cases'
                ' this is because you are using a point in time recipe or the three-'
                'phase recipe. You should use one of the daylight coefficient based '
                'recipes or the 5 phase recipe instead.')
        elif not self.has_direct_values:
            # results are not loaded but are available
            assert len(self.result_files[1]) == 1, \
                ValueError(
                    'Annual recipe can currently only handle '
                    'a single merged result file.'
            )
            results_loaded = False
            print('Loading the results from result files.')

        res = ([], [], [])
        threshhold = threshhold or 1000
        target_hours = target_hours or 250
        target_area = target_area or 10
        hoys = self.hoys
        occ_schedule = occ_schedule or set(hoys)

        if results_loaded:
            blinds_state_ids = blinds_state_ids or [[0] * len(self.sources)] * len(hoys)

            for sensor in self.analysis_points:
                for c, r in enumerate(sensor.annual_solar_exposure(threshhold,
                                                                   blinds_state_ids,
                                                                   occ_schedule,
                                                                   target_hours
                                                                   )):
                    res[c].append(r)
        else:
            # This is a method for annual recipe to load the results line by line
            # which unlike the other method doesn't load all the values to the memory
            # at once.
            blinds_state_ids = [[0] * len(self.sources)] * len(hoys)
            calculate_annual_solar_exposure = \
                self.analysis_points[0]._calculate_annual_solar_exposure

            for file_data in self.result_files[1]:
                file_path, hoys, start_line, header, mode = file_data

                # read the results line by line and caluclate the values
                if os.path.getsize(file_path) < 2:
                    raise EmptyFileError(file_path)

                assert mode == 0, \
                    TypeError(
                        'Annual results can only be calculated from '
                        'illuminance studies.')

                st = start_line or 0

                with open(file_path, 'rb') as inf:
                    if header:
                        inf, _ = self.parse_header(inf, st, hoys, False)

                    for i in xrange(st):
                        inf.next()

                    end = len(self._analysis_points)

                    # load one line at a time
                    for count in xrange(end):
                        values = (int(float(r)) for r in inf.next().split())
                        for c, r in enumerate(
                            calculate_annual_solar_exposure(
                                values, hoys, threshhold, blinds_state_ids, occ_schedule,
                                target_hours)):

                            res[c].append(r)

        # calculate ase for the grid
        ap = self.analysis_points  # create a local copy of points for better performance
        problematic_point_count = 0
        problematic_points = []
        problematic_hours = []
        ase_values = []
        for i, (success, ase, pHours) in enumerate(izip(*res)):
            ase_values.append(ase)  # collect annual ase values for each point
            if success:
                continue
            problematic_point_count += 1
            problematic_points.append(ap[i])
            problematic_hours.append(pHours)

        per_problematic = 100 * problematic_point_count / len(ap)
        return per_problematic < target_area, ase_values, per_problematic, \
            problematic_points, problematic_hours

    def parse_blind_states(self, blinds_state_ids):
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
        return self.analysis_points[0].parse_blind_states(blinds_state_ids)
