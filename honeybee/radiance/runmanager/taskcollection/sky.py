"""Collection of tasks and subtask for generating skies."""
from ..task import SubTask
from ...command.gendaymtx import Gendaymtx
import os


def genskymtx(project_folder, sky_matrix, sky_mode, re_use):
    """A subtask to generate a sky.

    Args:
        sky_mode: 0 > total, 1 > direct only, 2 > sky only
    """
    # # 2.1.Create sky matrix.
    sky_matrix.mode = sky_mode
    sky_mtx_file = 'sky/{}.smx'.format(sky_matrix.name)

    # add commands for total and direct sky matrix.
    if not hasattr(sky_matrix, 'isSkyMatrix'):
        raise TypeError('You must use a SkyMatrix to generate the sky.')

    wea_filepath = 'sky/{}.wea'.format(sky_matrix.name)
    sky_mtx = 'sky/{}.smx'.format(sky_matrix.name)
    hours_file = os.path.join(project_folder, 'sky/{}.hrs'.format(sky_matrix.name))

    if not os.path.isfile(os.path.join(project_folder, sky_mtx)) \
            or not os.path.isfile(os.path.join(project_folder, wea_filepath)) \
            or not sky_matrix.hours_match(hours_file):
        # write wea file to folder
        sky_matrix.write_wea(os.path.join(project_folder, 'sky'), write_hours=True)
        gdm = Gendaymtx(output_name=sky_mtx, wea_file=wea_filepath)
        gdm.gendaymtx_parameters = sky_matrix.sky_matrix_parameters

    gensky_st = SubTask(title='Generate sky', command=gdm.to_rad_string(),
                        output_file=sky_mtx_file)
    return gensky_st


def gensunmtx(wea):
    """generate analemma.

    This should probably stay as a helper function.
    """
    pass
