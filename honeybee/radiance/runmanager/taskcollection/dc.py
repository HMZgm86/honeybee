"""Collection of tasks and subtask for generating skies."""
from ..task import SubTask
from ...command.rfluxmtx import Rfluxmtx
import os


def sky_coeff(output_name, receiver, rad_files, sender, points_file=None,
              number_of_points=None, rfluxmtx_parameters=None):
    """Returns radiance commands to create coefficient matrix.

    Args:
        output_name: Output file name.
        receiver: A radiance file to indicate the receiver. In view matrix it will be the
        window group and in daylight matrix it will be the sky.
        rad_files: A collection of Radiance files that should be included in the scene.
        sender: A collection of files for senders if senders are radiance geometries
            such as window groups (Default: '-').
        points_file: Path to point file which will be used instead of sender.
        number_of_points: Number of points in points_file as an integer.
        rfluxmtx_parameters: Radiance parameters for Rfluxmtx command using a
            RfluxmtxParameters instance (Default: None).
    """
    sender = sender or '-'
    rad_files = rad_files or ()
    number_of_points = number_of_points or 0
    rfluxmtx = Rfluxmtx()

    if sender == '-':
        assert points_file, \
            ValueError('You have to set the points_file when sender is not defined.')

    # -------------- set the parameters ----------------- #
    rfluxmtx.rfluxmtx_parameters = rfluxmtx_parameters

    # -------------- set up the sender objects ---------- #
    # '-' in case of view matrix, window group in case of
    # daylight matrix. This is normally the receiver file
    # in view matrix
    rfluxmtx.sender = sender

    # points file are the senders in view matrix
    rfluxmtx.number_of_points = number_of_points
    rfluxmtx.points_file = points_file

    # --------------- set up the  receiver --------------- #
    # This will be the window for view matrix and the sky for
    # daylight matrix. It makes sense to make a method for each
    # of thme as they are pretty repetitive
    # Klems full basis sampling
    rfluxmtx.receiver_file = receiver

    # ------------- add radiance geometry files ----------------
    # For view matrix it's usually the room itself and the materials
    # in case of each view analysis rest of the windows should be
    # blacked! In case of daylight matrix it will be the context
    # outside the window.
    rfluxmtx.rad_files = rad_files

    # output file address\name
    rfluxmtx.output_matrix = output_name

    st = SubTask(title='Coefficient calculation',
                 command=rfluxmtx.to_rad_string(),
                 output_file=output_name,
                 expected_output_size=number_of_points * 4)
    return st


def sun_coeff(sun_matrix_file):
    pass
