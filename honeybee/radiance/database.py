"""Database for daylight simulation results.

Read more at:
"""
import sqlite3 as lite
import os
from itertools import izip
import contextlib
from collections import OrderedDict


class SqliteDB(object):
    """Sqlite3 database for honeybee grid_based daylight simulation.

    The database currently only supports grid-based simulation and image-based will
    be added in the near future.
    """

    project_table_schema = """CREATE TABLE IF NOT EXISTS Project (
          id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
          name TEXT NOT NULL,
          created_at TIMESTAMP NOT NULL,
          values_loaded INTEGER NOT NULL,
          dir_values_loaded INTEGER NOT NULL
          );"""

    # sensors and analysis grids.
    sensor_table_schema = """CREATE TABLE IF NOT EXISTS Sensor (
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
              loc_x REAL NOT NULL,
              loc_y REAL NOT NULL,
              loc_z REAL NOT NULL,
              dir_x REAL NOT NULL,
              dir_y REAL NOT NULL,
              dir_z REAL NOT NULL
              );"""

    grid_table_schema = """CREATE TABLE IF NOT EXISTS Grid (
              id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
              name TEXT,
              project_id INTEGER,
              FOREIGN KEY (project_id) REFERENCES Project(id)
              );"""

    sensor_grid_table_schema = """CREATE TABLE IF NOT EXISTS SensorGrid (
              sensor_id INTEGER,
              grid_id INTEGER,
              FOREIGN KEY (sensor_id) REFERENCES Sensor(id),
              FOREIGN KEY (grid_id) REFERENCES Grid(id)
              );"""

    # light sources
    source_table_schema = """CREATE TABLE IF NOT EXISTS Source (
              id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
              name TEXT
              );"""

    state_table_schema = """CREATE TABLE IF NOT EXISTS State (
              id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
              name TEXT
              );"""

    source_state_table_schema = """CREATE TABLE IF NOT EXISTS SourceState (
              source_id INTEGER,
              state_id INTEGER,
              FOREIGN KEY (source_id) REFERENCES Source(id),
              FOREIGN KEY (state_id) REFERENCES State(id)
              );"""

    # light sources and analysis grids relationship
    source_grid_table_schema = """CREATE TABLE IF NOT EXISTS SourceGrid (
              source_id INTEGER NOT NULL,
              grid_id INTEGER NOT NULL,
              FOREIGN KEY (source_id) REFERENCES Source(id),
              FOREIGN KEY (grid_id) REFERENCES Grid(id)
              );"""

    # daylight analysis results
    result_table_schema = """CREATE TABLE IF NOT EXISTS Result (
              sensor_id INTEGER NOT NULL,
              source_id INTEGER NOT NULL,
              state_id INTEGER NOT NULL,
              hoy REAL NOT NULL,
              sky INTEGER,
              direct INTEGER,
              sun INTEGER,
              FOREIGN KEY (sensor_id) REFERENCES Sensor(id),
              FOREIGN KEY (source_id) REFERENCES Source(id),
              FOREIGN KEY (state_id) REFERENCES State(id),
              CONSTRAINT result_id PRIMARY KEY (sensor_id, source_id, state_id, hoy)
              );"""

    def __init__(self, folder, filename='radout', clean_if_exist=False):
        """Initate database.

        Args:
            folder: Path to folder to create database.
            filename: Optional database filename (default:radout)
            clean_if_exist: Clean the data in database file if the file exist
                (default: False).
        """
        self.filepath = os.path.join(folder, '%s.db' % filename)
        if os.path.isfile(self.filepath):
            if clean_if_exist:
                # database exists just clean it
                self.clean()
        else:
            conn = lite.connect(self.filepath)
            conn.execute('PRAGMA synchronous=OFF')
            c = conn.cursor()
            # create table for sensors
            c.execute(self.project_table_schema)
            c.execute(self.sensor_table_schema)
            c.execute(self.grid_table_schema)
            c.execute(self.sensor_grid_table_schema)

            # create table for sources and place holder for results
            c.execute(self.source_table_schema)
            c.execute(self.state_table_schema)
            c.execute(self.source_state_table_schema)
            c.execute(self.source_grid_table_schema)

            c.execute(self.result_table_schema)
            conn.commit()
            conn.close()

    @classmethod
    def from_analysis_recipe(cls, analysis_recipe, folder, filename='radout'):
        cls_ = cls(folder, filename)
        # TODO(mostapha): fill the data from recipe.

        return cls_

    @property
    def isDataBase(self):
        """Return True for database object."""
        return True

    @property
    def has_values(self):
        """A Boolean to indicate if the values are loaded."""
        conn = lite.connect(self.filepath)
        c = conn.cursor()
        c.execute("SELECT values_loaded FROM Project")
        return bool(c.fetchone()[0])
        conn.close()

    @property
    def tables(self):
        """Get list of tables."""
        conn = lite.connect(self.filepath)
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = tuple(table[0] for table in tables)
        conn.close()
        return tables

    def execute(self, command, values=None):
        """Run sql command."""
        with contextlib.closing(lite.connect(self.filepath)) as conn:
            with conn:
                with contextlib.closing(conn.cursor()) as cursor:
                    if values:
                        cursor.execute(command, values)
                    else:
                        cursor.execute(command)
                    return cursor.fetchall()

    def executemany(self, command, values=None):
        """Run sql command."""
        with contextlib.closing(lite.connect(self.filepath)) as conn:
            with conn:
                with contextlib.closing(conn.cursor()) as cursor:
                    if values:
                        cursor.executemany(command, values)
                    else:
                        cursor.executemany(command)
                    return cursor.fetchall()

    def is_column(self, table_name, column_name):
        """Check if a column is available in a table in this database."""
        cmd = "PRAGMA table_info(%s)" % table_name
        return column_name in tuple(i[1] for i in self.execute(cmd))

    def clean(self):
        """Clean the current data from the table."""
        tables = self.tables
        conn = lite.connect(self.filepath)
        c = conn.cursor()
        # clean data in each db
        for table in tables:
            c.execute("DELETE FROM %s" % table)

        c.execute("VACUUM Results")
        conn.commit()
        conn.close()

    @property
    def last_sensor_id(self):
        command = """SELECT seq FROM sqlite_sequence WHERE name='Sensor';"""
        sensor_id = self.execute(command)
        return int(sensor_id[0][0])

    @property
    def last_state_id(self):
        command = """SELECT seq FROM sqlite_sequence WHERE name='State';"""
        state_id = self.execute(command)
        return int(state_id[0][0])

    @property
    def last_source_id(self):
        command = """SELECT seq FROM sqlite_sequence WHERE name='Source';"""
        source_id = self.execute(command)
        return int(source_id[0][0])

    @property
    def last_grid_id(self):
        command = """SELECT seq FROM sqlite_sequence WHERE name='Grid';"""
        grid_id = self.execute(command)
        return int(grid_id[0][0])

    @property
    def last_project_id(self):
        command = """SELECT seq FROM sqlite_sequence WHERE name='Project';"""
        project_id = self.execute(command)
        return int(project_id[0][0])

    def add_analysis_grid(self, analysis_grid, hoys=None):
        """Add an analysis grid to database.

        This method adds the analysis grid, sources associated with this analysis grid,
        and states for each source to the database. Also adds place holder for results
        for each analysis point in this analysis grid and for each hour in hoys.
        """
        # find the id for the last analysis grid
        # create grid table
        # create sensor points
        point_file = r"C:\ladybug\sample_files\gridbased_daylightcoeff\sample_files.pts"
        sensor_command = """
            INSERT INTO Sensor (id, loc_x, loc_y, loc_z, dir_x, dir_y, dir_z)
            VALUES (?, ?, ?, ?, ?, ?, ?);"""
        with open(point_file, 'rb') as inf:
            values = ([count] + [float(i) for i in sensor_data.split()]
                      for count, sensor_data in enumerate(inf))
            self.executemany(sensor_command, values)

        command = """INSERT INTO SensorGrid (sensor_id, grid_id) VALUES (?, ?)"""
        with open(point_file, 'rb') as inf:
            # add sensor to grid
            values = ((sensor_id, 0) for sensor_id, _ in enumerate(inf))

            self.executemany(command, values)

    def add_light_sources(self, light_sources):
        """Add light sources to database."""
        #
        # add room grid to all grid table. This is just a place holder
        grid_command = """INSERT INTO Grid (id, name) VALUES (?, ?)"""
        # c.execute(grid_command, (0, 'room'))
        #
        # for state_id, state in enumerate(states):
        #     command = """INSERT INTO State (id, name) VALUES (?, ?);"""
        #     c.execute(command, (state_id, state))
        #
        # for source_id, source in enumerate(sources):
        #     # add source to grid
        #     command = """INSERT INTO SourceGrid (source_id, grid_id) VALUES (?, ?)"""
        #     c.execute(command, (source_id, 0))
        #     # add sources to tables
        #     command = """INSERT INTO Source (id, name) VALUES (?, ?);"""
        #     c.execute(command, (source_id, source))
        #     # put sources and state together
        #     for state in sources[source]:
        #         state_id = states.index(state)
        #         command = """INSERT INTO SourceState (source_id, state_id) VALUES (?, ?);"""
        #         c.execute(command, (source_id, state_id))

    def add_results_place_holder(self, light_sources, hoys=None):
        """add the place holder for results which will be filled
        after analysis is over."""
        # find unique sources from light_sources
        sources = []
        states = []
        hoys = hoys or xrange(8760)
        command = """
            INSERT INTO Result (sensor_id, source_id, state_id, hoy)
            VALUES (?, ?, ?, ?);
        """
        sensor_id = self.last_sensor_id
        values = []
        for pt_count, _ in xrange(sensor_id):
            for hoy in hoys:
                for source_id, source in enumerate(sources):
                    for state in sources[source]:
                        state_id = states.index(state)
                        # c.execute(command, (pt_count, source_id, state_id, hoy))
                        values.append((pt_count, source_id, state_id, hoy))
        self.executemany(command, values)

    def load_results_from_folder(self, folder=None):
        """Load the results from folder.

        This method looks for files with .ill extensions. The file should be named as
        <data - type > .. < window - group - name > .. < state - name > .ill for instance
        total..north_facing..default.ill includes the 'total' values from 'north_facing'
        window group at 'default' state.

        Args:
            folder:
            window_groups:
        """
        update_values = """UPDATE Result SET sky=?, direct=?, sun=?
            WHERE sensor_id=? AND source_id=? AND state_id=? AND hoy=?"""
        if not folder:
            os.path.dirname(self.filepath)

        files = tuple(f for f in os.listdir(folder) if f.startswith('total'))
        sources = tuple(fl[:-4].split('..')[1] for fl in files)
        states = ('default', 'dark_glass_0.25')
        for f in files:
            # load all the files at the same time
            total_file = os.path.join(folder, f)
            direct_file = os.path.join(folder, f.replace('total..', 'direct..'))
            sun_file = os.path.join(folder, f.replace('total..', 'sun..'))
            _, source, state = f[:-4].split('..')
            source_id = sources.index(source)
            state_id = states.index(state)
            values = []
            with open(total_file, 'rb') as tinf, \
                    open(direct_file, 'rb') as dinf, \
                    open(sun_file, 'rb') as sinf:
                for i in range(7):
                    tinf.next()
                    dinf.next()
                    sinf.next()
                for sensor_id, (tl, dl, sl) in enumerate(izip(tinf, dinf, sinf)):
                    for hour, (t, d, s) in \
                            enumerate(izip(tl.split(), dl.split(), sl.split())):
                        values.append((int(float(t)), int(float(d)), int(float(s)),
                                       sensor_id, source_id, state_id, hour))
                # execute all the value changes
                self.executemany(update_values, values)

    def load_results_from_file(self, file_path, source=None, state=None, data_type=0):
        """Load results from a single .ill file.

        Each line should be the results for an hour.
        """
        pass

    def set_values(self, hoys, values, source=None, state=None, is_direct=False):

        pass
        # assign the values to points
        for count, hourlyValues in enumerate(values):
            self.analysis_points[count].set_values(
                hourlyValues, hoys, source, state, is_direct)

    def parse_header(self, inf, start_line, hoys, check_point_count=False):
        """Parse radiance matrix header."""
        # read the header
        for i in xrange(10):
            line = inf.next()
            if line[:6] == 'FORMAT':
                inf.next()  # pass empty line
                break  # done with the header!
            elif start_line == 0 and line[:5] == 'NROWS':
                points_count = int(line.split('=')[-1])
                if check_point_count:
                    assert len(self._analysis_points) == points_count, \
                        "Length of points [{}] must match the number " \
                        "of rows [{}].".format(
                            len(self._analysis_points), points_count)

            elif start_line == 0 and line[:5] == 'NCOLS':
                hours_count = int(line.split('=')[-1])
                if hoys:
                    assert hours_count == len(hoys), \
                        "Number of hours [{}] must match the " \
                        "number of columns [{}]." \
                        .format(len(hoys), hours_count)
                else:
                    hoys = xrange(0, hours_count)

        return inf, hoys

    def set_values_from_file(self, file_path, hoys=None, source=None, state=None,
                             start_line=None, is_direct=False, header=True,
                             check_point_count=True, mode=0):
        """Load values for test points from a file.

        Args:
            file_path: Full file path to the result file.
            hoys: A collection of hours of the year for the results. If None the
                default will be range(0, len(results)).
            source: Name of the source.
            state: Name of the state.
            start_line: Number of start lines after the header from 0 (default: 0).
            is_direct: A Boolean to declare if the results is direct illuminance
                (default: False).
            header: A Boolean to declare if the file has header (default: True).
            mode: 0 > load the values 1 > load values as binary. Any non-zero value
                will be 1. This is useful for studies such as sunlight hours. 2 >
                load the values divided by mode number. Use this mode for daylight
                factor or radiation analysis.
        """

        if os.path.getsize(file_path) < 2:
            raise EmptyFileError(file_path)

        st = start_line or 0

        with open(file_path, 'rb') as inf:
            if header:
                inf, hoys = self.parse_header(inf, st, hoys, check_point_count)

            self.add_result_files(file_path, hoys, st, is_direct, header, mode)

            for i in xrange(st):
                inf.next()

            end = len(self._analysis_points)
            if mode == 0:
                values = (tuple(int(float(r)) for r in inf.next().split())
                          for count in xrange(end))
            elif mode == 1:
                # binary 0-1
                values = (tuple(1 if float(r) > 0 else 0 for r in inf.next().split())
                          for count in xrange(end))
            else:
                # divide values by mode (useful for daylight factor calculation)
                values = (tuple(int(float(r) / mode) for r in inf.next().split())
                          for count in xrange(end))

            # assign the values to points
            for count, hourlyValues in enumerate(values):
                self.analysis_points[count].set_values(
                    hourlyValues, hoys, source, state, is_direct)

    def set_coupled_values_from_file(
            self, total_file_path, direct_file_path, hoys=None, source=None, state=None,
            start_line=None, header=True, check_point_count=True, mode=0):
        """Load direct and total values for test points from two files.

        Args:
            file_path: Full file path to the result file.
            hoys: A collection of hours of the year for the results. If None the
                default will be range(0, len(results)).
            source: Name of the source.
            state: Name of the state.
            start_line: Number of start lines after the header from 0 (default: 0).
            header: A Boolean to declare if the file has header (default: True).
            mode: 0 > load the values 1 > load values as binary. Any non-zero value
                will be 1. This is useful for studies such as sunlight hours. 2 >
                load the values divided by mode number. Use this mode for daylight
                factor or radiation analysis.
        """

        for file_path in (total_file_path, direct_file_path):
            if os.path.getsize(file_path) < 2:
                raise EmptyFileError(file_path)

        st = start_line or 0

        with open(total_file_path, 'rb') as inf, open(direct_file_path, 'rb') as dinf:
            if header:
                inf, hoys = self.parse_header(inf, st, hoys, check_point_count)
                dinf, hoys = self.parse_header(dinf, st, hoys, check_point_count)

            self.add_result_files(total_file_path, hoys, st, False, header, mode)
            self.add_result_files(direct_file_path, hoys, st, True, header, mode)

            for i in xrange(st):
                inf.next()
                dinf.next()

            end = len(self._analysis_points)

            if mode == 0:
                coupled_values = (
                    tuple((int(float(r)), int(float(d))) for r, d in
                          izip(inf.next().split(), dinf.next().split()))
                    for count in xrange(end))
            elif mode == 1:
                # binary 0-1
                coupled_values = (tuple(
                    (int(float(1 if float(r) > 0 else 0)),
                     int(float(1 if float(d) > 0 else 0)))
                    for r, d in izip(inf.next().split(), dinf.next().split()))
                    for count in xrange(end))
            else:
                # divide values by mode (useful for daylight factor calculation)
                coupled_values = (
                    tuple((int(float(r) / mode), int(float(d) / mode)) for r, d in
                          izip(inf.next().split(), dinf.next().split()))
                    for count in xrange(end))

            # assign the values to points
            for count, hourlyValues in enumerate(coupled_values):
                self.analysis_points[count].set_coupled_values(
                    hourlyValues, hoys, source, state)

    def load_values_from_files(self):
        """Load grid values from self.result_files."""
        # remove old results
        for ap in self._analysis_points:
            ap._sources = OrderedDict()
            ap._values = []
        r_files = self.result_files[0][:]
        d_files = self.result_files[1][:]
        self._totalFiles = []
        self._directFiles = []
        # pass
        if r_files and d_files:
            # both results are available
            for rf, df in izip(r_files, d_files):
                rfPath, hoys, start_line, header, mode = rf
                dfPath, hoys, start_line, header, mode = df
                fn = os.path.split(rfPath)[-1][:-4].split("..")
                source = fn[-2]
                state = fn[-1]
                print(
                    '\nloading total and direct results for {} AnalysisGrid'
                    ' from {}::{}\n{}\n{}\n'.format(
                        self.name, source, state, rfPath, dfPath))
                self.set_coupled_values_from_file(
                    rfPath, dfPath, hoys, source, state, start_line, header,
                    False, mode
                )
        elif r_files:
            for rf in r_files:
                rfPath, hoys, start_line, header, mode = rf
                fn = os.path.split(rfPath)[-1][:-4].split("..")
                source = fn[-2]
                state = fn[-1]
                print('\nloading the results for {} AnalysisGrid form {}::{}\n{}\n'
                      .format(self.name, source, state, rfPath))
                self.set_values_from_file(
                    rf, hoys, source, state, start_line, is_direct=False,
                    header=header, check_point_count=False, mode=mode
                )
        elif d_files:
            for rf in d_files:
                rfPath, hoys, start_line, header, mode = rf
                fn = os.path.split(rfPath)[-1][:-4].split("..")
                source = fn[-2]
                state = fn[-1]
                print('\nloading the results for {} AnalysisGrid form {}::{}\n{}\n'
                      .format(self.name, source, state, rfPath))
                self.set_values_from_file(
                    rf, hoys, source, state, start_line, is_direct=True,
                    header=header, check_point_count=False, mode=mode
                )
