from semantique.blocks import *
from semantique.extent import SpatialExtent, TemporalExtent
from semantique.recipe import QueryRecipe

import semantique.factbase
import semantique.ontology

import pkg_resources

try:
  __version__ = pkg_resources.get_distribution("semantique").version
except Exception:
  __version__ = "unknown"