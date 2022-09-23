__copyright__ = "© 2022 innoCampus, Technische Universität Berlin"
__license__ = """GNU GPLv3+

This file is part of videbo.

videbo is free software: you can redistribute it and/or modify it under the terms of
the GNU General Public License as published by the Free Software Foundation,
either version 3 of the License, or (at your option) any later version.

videbo is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with videbo.
If not, see <https://www.gnu.org/licenses/>."""

__version__ = "0.1.0"

__doc__ = """
Top-level global variables for settings singletons for convenient imports.

Settings are overwritten in the `__main__` script.
"""

from .distributor.settings import DistributorSettings as _DistributorSettings
from .storage.settings import StorageSettings as _StorageSettings


distributor_settings = _DistributorSettings()
storage_settings = _StorageSettings()
