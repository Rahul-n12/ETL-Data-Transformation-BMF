import shutil
import os
import glob
from setuptools import Command, setup

package_name = 'bmf'


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    CLEAN_FILES = './build ./dist ./*.pyc ./*.tgz ./*.egg-info'.split(' ')

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        global package_name

        for path_spec in self.CLEAN_FILES:
            # Make paths absolute and relative to this path
            abs_paths = glob.glob(os.path.normpath(os.path.join(package_name, path_spec)))
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(package_name):
                    # Die if path in CLEAN_FILES is absolute + outside this directory
                    raise ValueError("%s is not a path inside %s" % (path, package_name))
                print('removing %s' % os.path.relpath(path))
                shutil.rmtree(path)


setup(
    name=package_name,
    description='A simulation of Migration Specialist',
    long_description=(
        'A simulation of a BASE Migration Framework '
        'to use in internal technical assesments of applicants'
    ),
    version='0.0.1',
    packages=[package_name],
    install_requires=[
        'openpyxl',
        'pandas',
        'PyYAML',
        'tqdm',
    ],
    cmdclass={'clean': CleanCommand}
)
