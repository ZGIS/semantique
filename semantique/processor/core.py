import geopandas as gpd
import numpy as np

import copy
import inspect
import logging
import pyproj
import pytz
import warnings

import semantique.processor.operators
import semantique.processor.reducers
from semantique import exceptions
from semantique.processor import structures, utils

logger = logging.getLogger(__name__)

class QueryProcessor():

  def __init__(self, recipe, factbase, ontology, extent, operators = None,
               extra_operators = None, reducers = None, extra_reducers = None,
               track_types = True, trim_filter = False, trim_results = True,
               unstack_results = True):
    self._eval_obj = [None]
    self._response = {}
    self.recipe = recipe
    self.factbase = factbase
    self.ontology = ontology
    self.extent = extent
    self.track_types = track_types
    self.trim_filter = trim_filter
    self.trim_results = trim_results
    self.unstack_results = unstack_results
    if operators is None:
      self.store_default_operators()
    else:
      self.operators = operators
    if extra_operators is not None:
      self.operators.update(extra_operators)
    if reducers is None:
      self.store_default_reducers()
    else:
      self.reducers = reducers
    if extra_reducers is not None:
      self.reducers.update(extra_reducers)

  @property
  def recipe(self):
    return self._recipe

  @recipe.setter
  def recipe(self, value):
    self._recipe = value

  @property
  def factbase(self):
    return self._factbase

  @factbase.setter
  def factbase(self, value):
    self._factbase = value

  @property
  def ontology(self):
    return self._ontology

  @ontology.setter
  def ontology(self, value):
    self._ontology = value

  @property
  def extent(self):
    return self._extent

  @extent.setter
  def extent(self, value):
    self._extent = value
    self._eval_obj[0] = value

  @property
  def crs(self):
    return self._extent.sq.crs

  @property
  def spatial_resolution(self):
    return self._extent.sq.spatial_resolution

  @property
  def tz(self):
    return self._extent.sq.tz

  @property
  def operators(self):
    return self._operators

  @operators.setter
  def operators(self, value):
    self._operators = value

  @property
  def reducers(self):
    return self._reducers

  @reducers.setter
  def reducers(self, value):
    self._reducers = value

  @property
  def track_types(self):
    return self._track_types

  @track_types.setter
  def track_types(self, value):
    self._track_types = value

  @property
  def trim_filter(self):
    return self._trim_filter

  @trim_filter.setter
  def trim_filter(self, value):
    self._trim_filter = value

  @property
  def trim_results(self):
    return self._trim_results

  @trim_results.setter
  def trim_results(self, value):
    self._trim_results = value

  @property
  def unstack_results(self):
    return self._unstack_results

  @unstack_results.setter
  def unstack_results(self, value):
    self._unstack_results = value

  @classmethod
  def parse(cls, recipe, factbase, ontology, space, time, **config):
    # Step I: Parse the spatio-temporal extent.
    # It needs to be stored as a 2D array with dimensions space and time.
    # First extract the relevant configuration parameters.
    config = copy.deepcopy(config)
    spatres = config.pop("spatial_resolution", [-10, 10])
    crs = config.pop("output_crs", None)
    tz = config.pop("output_tz", None)
    # Create the extent cube.
    extent = utils.create_extent_cube(
      spatial_extent = space,
      temporal_extent = time,
      spatial_resolution = spatres,
      crs = crs,
      tz = tz,
      trim = trim
    )
    # Step II: Initialize the QueryProcessor instance.
    return cls(recipe, factbase, ontology, extent, **config)

  def optimize(self):
    return self

  def execute(self):
    # Step I: Execute the instructions for each result iteratively.
    for x in self._recipe:
      if x in self._response:
        continue
      result = self.call_handler(self._recipe[x])
      result.name = x
      self._response[x] = result
    # Step II: Return the response.
    # Trim result arrays if requested.
    # This means we drop all coordinates for which all values are nan.
    if self._trim_results:
      def trim(obj):
        try:
          obj = obj.sq
        except AttributeError:
          pass
        return obj.trim(trim_space = True, force_regular = True)
      self._response = {k: trim(v) for k, v in self._response.items()}
    # Unstack spatial dimensions if requested.
    if self._unstack_results:
      def unstack(obj):
        try:
          obj = obj.sq
        except AttributeError:
          pass
        return obj.unstack_spatial_dims()
      self._response = {k: unstack(v) for k, v in self._response.items()}
    return self._response

  def call_handler(self, block):
    out = self.get_handler(block)(block)
    logger.debug(f"Handled {btype}:\n{out}")
    return out

  def get_handler(self, block):
    try:
      btype = block["type"]
    except TypeError:
      raise exceptions.InvalidBuildingBlockError(
        "Block is not subscriptable"
      )
    except KeyError:
      raise exceptions.InvalidBuildingBlockError(
        "Block has no 'type' key"
      )
    try:
      out = getattr(self, "handle_" + btype)
    except AttributeError:
      raise exceptions.InvalidBuildingBlockError(
        f"Unknown block type: '{btype}'"
      )
    return out

  def handle_concept(self, block):
    out = self._ontology.translate(
      *block["reference"],
      property = block["property"] if "property" in block else None,
      extent = self._extent,
      factbase = self._factbase,
      eval_obj = self._get_eval_obj(),
      operators = self._operators,
      reducers = self._reducers,
      track_types = self._track_types,
      trim_filter = self._trim_filter
    )
    return out

  def handle_resource(self, block):
    out = self._factbase.retrieve(
      *block["reference"],
      extent = self._extent
    )
    return out

  def handle_result(self, block):
    name = block["name"]
    if name not in self._response:
      try:
        instructions = self._recipe[name]
      except KeyError:
        raise exceptions.UnknownReferenceError(
          f"Recipe does not contain result '{name}'"
        )
      result = self.call_handler(instructions)
      result.name = name
      self._response[name] = result
      return result
    return self._response[name]

  def handle_self(self, block):
    return self._get_eval_obj()

  def handle_processing_chain(self, block):
    obj = self.call_handler(block["with"])
    self._set_eval_obj(obj)
    for i in block["do"]:
      out = self.call_handler(i)
    self._reset_eval_obj()
    return out

  def handle_collection(self, block):
    out = [self.call_handler(x) for x in block["elements"]]
    return structures.CubeCollection(out)

  def handle_value_label(self, block):
    label = block["label"]
    try:
      idx = self._get_eval_obj().sq.value_labels[label]
    except (KeyError, TypeError):
      raise exceptions.UnknownLabelError(
        f"Category label '{label}' is not defined"
      )
    return idx

  def handle_geometries(self, block):
    feats = block["value"]["features"]
    crs = pyproj.CRS.from_string(block["value"]["crs"])
    geodf = gpd.GeoDataFrame.from_features(feats, crs = crs)
    if crs != self.crs:
      geodf = geodf.to_crs(crs)
    out = xr.DataArray([geodf])
    out.sq.value_type = "space"
    return out

  def handle_time_instant(self, block):
    dt = np.datetime64(block["value"]["datetime"])
    tz = pytz.timezone(block["value"]["tz"])
    if tz != self.tz.zone:
      dt = utils.convert_datetime64(dt, tz, self.tz)
    out = xr.DataArray([dt])
    out.sq.value_type = "time"
    return out

  def handle_time_interval(self, block):
    start = np.datetime64(block["value"]["start"])
    end = np.datetime64(block["value"]["end"])
    tz = pytz.timezone(block["value"]["tz"])
    if tz != self.tz:
      start = utils.convert_datetime64(start, tz, self.tz)
      end = utils.convert_datetime64(start, tz, self.tz)
    out = xr.DataArray([start, end])
    out.sq.value_type = "time"
    return out

  def handle_verb(self, block):
    name = block["name"]
    try:
      params = block["params"]
    except KeyError:
      params = {}
    try:
      params = getattr(self, "update_params_of_" + name)(params)
    except AttributeError:
      pass
    obj = self._get_eval_obj()
    try:
      obj = obj.sq
    except AttributeError:
      pass
    out = getattr(obj, name)(**params)
    try:
      is_empty = out.sq.is_empty
    except:
      is_empty = out.is_empty
    if is_empty:
      warnings.warn(
        f"Verb '{name}' returned an empty array"
      )
    self._replace_eval_obj(out)
    return out

  def update_params_of_evaluate(self, params):
    params = copy.deepcopy(params)
    try:
      y = params["y"]
    except KeyError:
      pass
    else:
      if isinstance(y, (list, tuple)):
        params["y"] = self.update_list_elements(y)
      else:
        try:
          params["y"] = self.call_handler(y)
        except exceptions.InvalidBuildingBlockError:
          pass
    params["operator"] = self.get_operator(params["operator"])
    params["track_types"] = self._track_types
    return params

  def update_params_of_filter(self, params):
    params = copy.deepcopy(params)
    filterer = params["filterer"]
    params["filterer"] = getattr(self, "handle_" + filterer["type"])(filterer)
    params["trim"] = self._trim_filter
    params["track_types"] = self._track_types
    return params

  def update_params_of_groupby(self, params):
    params = copy.deepcopy(params)
    grouper = params["grouper"]
    params["grouper"] = getattr(self, "handle_" + grouper["type"])(grouper)
    return params

  def update_params_of_reduce(self, params):
    params = copy.deepcopy(params)
    params["reducer"] = self.get_reducer(params["reducer"])
    params["track_types"] = self._track_types
    return params

  def update_params_of_replace(self, params):
    params = copy.deepcopy(params)
    y = params["y"]
    try:
      params["y"] = self.call_handler(y)
    except ValueError:
      if isinstance(y, (list, tuple)):
        params["y"] = self.update_list_elements(y)
      else:
        pass
    params["track_types"] = self._track_types
    return params

  def update_params_of_compose(self, params):
    params = copy.deepcopy(params)
    params["track_types"] = self._track_types
    return params

  def update_params_of_concatenate(self, params):
    params = copy.deepcopy(params)
    params["track_types"] = self._track_types
    return params

  def update_params_of_merge(self, params):
    params = copy.deepcopy(params)
    params["reducer"] = self.get_reducer(params["reducer"])
    params["track_types"] = self._track_types
    return params

  def update_list_elements(self, obj):
    def _update(x):
      try:
        return self.call_handler(x)
      except exceptions.InvalidBuildingBlockError:
        return x
    return [_update(x) for x in obj]

  def add_operator(self, name, function):
    new = {name: function}
    self._operators.update(new)

  def get_operator(self, name):
    try:
      obj = self._operators[name]
    except KeyError:
      raise exceptions.UnknownOperatorError(
        f"Operator '{name}' is not defined"
      )
    return obj

  def store_default_operators(self):
    src = semantique.processor.operators
    functions = dict(inspect.getmembers(src, inspect.isfunction))
    functions = {k:v for k, v in functions.items() if k[0] != "_"}
    out = {}
    for f in functions:
      out[f[:-1]] = functions[f]
    self._operators = out

  def add_reducer(self, name, function):
    new = {name: function}
    self._reducers.update(new)

  def get_reducer(self, name):
    try:
      obj = self._reducers[name]
    except KeyError:
      raise exceptions.UnknownReducerError(
        f"Reducer '{name}' is not defined"
      )
    return obj

  def store_default_reducers(self):
    src = semantique.processor.reducers
    functions = dict(inspect.getmembers(src, inspect.isfunction))
    functions = {k:v for k, v in functions.items() if k[0] != "_"}
    out = {}
    for f in functions:
      out[f[:-1]] = functions[f]
    self._reducers = out

  def _get_eval_obj(self):
    return self._eval_obj[-1]

  def _replace_eval_obj(self, obj):
    self._eval_obj[-1] = obj

  def _reset_eval_obj(self):
    del self._eval_obj[-1]

  def _set_eval_obj(self, obj):
    self._eval_obj.append(obj)