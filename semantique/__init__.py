from semantique.blocks import *
from semantique.extent import SpatialExtent, TemporalExtent
from semantique.recipe import QueryRecipe

import semantique.datacube
import semantique.mapping
import semantique.operators
import semantique.reducers
import semantique.dimensions
import semantique.components

import pkg_resources

try:
  __version__ = pkg_resources.get_distribution("semantique").version
except Exception:
  __version__ = "unknown"