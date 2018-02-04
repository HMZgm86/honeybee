"""Database for daylight simulation results.

Read more at:
"""
import sqlite3 as lite
import os
from itertools import izip
import contextlib
from collections import OrderedDict


class SqliteDB(object):
    """Sqlite3 database for radiance based grid-based simulation."""

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
