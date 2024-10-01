import geopandas as gpd
import numpy as np
import pandas as pd
import copy
import inspect
import logging
import pystac
import pyproj
import pytz
import warnings
import xarray as xr

from semantique import datacube
from semantique import exceptions
from semantique.dimensions import TIME, SPACE, X, Y
from semantique.processor import arrays, operators, reducers, values, utils

logger = logging.getLogger(__name__)

class QueryProcessor():
  """Worker that takes care of processing a semantic query.

  Parameters
  ----------
    recipe : QueryRecipe
      The query recipe to be processed.
    datacube : Datacube
      The datacube instance to process the query against.
    mapping : Mapping
      The mapping instance to process the query against.
    extent : :obj:`xarray.DataArray`
      The spatio-temporal extent in which the query should be processed. Should
      be given as an array with a temporal dimension and two spatial dimensions
      such as returned by
      :func:`parse_extent <semantique.processor.utils.parse_extent>`.
    custom_verbs : :obj:`dict`, optional
      User-defined verbs that may be used when executing the query recipe in
      addition to the built-in verbs in semantique.
    custom_operators : :obj:`dict`, optional
      User-defined operator functions that may be used when evaluating
      expressions with the evaluate verb in addition to the built-in operators
      in semantique. Built-in operators with the same name will be overwritten.
    custom_reducers : :obj:`dict`, optional
      User-defined reducer functions that may be used when reducing array
      dimensions with the reduce verb in addition to the built-in reducers in
      semantique. Built-in reducers with the same name will be overwritten.
    track_types : :obj:`bool`
      Should the query processor keep track of the value type of arrays
      when applying processes, and promote them if necessary? Keeping track of
      value types also means throwing errors whenever a value type is not
      supported by a specific process.
    preview : :obj:`bool`
      Run the query processor with reduced resolution to test the recipe execution.
      Preview-runs are necessary if cache should be used.
    cache : :obj:`Cache`
      The cache object that is used to store data layers.
  """

  def __init__(self, recipe, datacube, mapping, extent, custom_verbs = None,
               custom_operators = None, custom_reducers = None,
               track_types = True, preview = False, cache = None):
    self._eval_obj = [None]
    self._response = {}
    self.recipe = recipe
    self.datacube = datacube
    self.mapping = mapping
    self.extent = extent
    self.track_types = track_types
    self.custom_verbs = custom_verbs
    self.custom_operators = custom_operators
    self.custom_reducers = custom_reducers
    self.preview = preview
    if cache is None:
      self.cache = Cache()
    else:
      self.cache = cache

  @property
  def response(self):
    """:obj:`dict`: Response of semantic query execution."""
    return self._response

  @property
  def recipe(self):
    """QueryRecipe: The query recipe to be processed."""
    return self._recipe

  @recipe.setter
  def recipe(self, value):
    self._recipe = value

  @property
  def datacube(self):
    """Datacube: The datacube instance to process the query
    against."""
    return self._datacube

  @datacube.setter
  def datacube(self, value):
    self._datacube = value

  @property
  def mapping(self):
    """Mapping: The mapping instance to process the query
    against."""
    return self._mapping

  @mapping.setter
  def mapping(self, value):
    self._mapping = value

  @property
  def extent(self):
    """:obj:`xarray.DataArray`: The spatio-temporal extent in which the query
    should be processed."""
    return self._extent

  @extent.setter
  def extent(self, value):
    self._extent = value
    self._eval_obj[0] = value

  @property
  def crs(self):
    """:obj:`pyproj.crs.CRS`: Spatial coordinate reference system in which the
    query should be processed."""
    return self._extent.sq.crs

  @property
  def spatial_resolution(self):
    """:obj:`list`: Spatial resolution in which the query should be
    processed."""
    return self._extent.sq.spatial_resolution

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Time zone in which the query should be
    processed."""
    return self._extent.sq.tz

  @property
  def custom_verbs(self):
    """:obj:`dict`: User-defined verbs for query execution."""
    return self._custom_verbs

  @custom_verbs.setter
  def custom_verbs(self, value):
    self._custom_verbs = {} if value is None else value

  @property
  def custom_operators(self):
    """:obj:`dict`: User-defined operator functions for the evaluate verb."""
    return self._custom_operators

  @custom_operators.setter
  def custom_operators(self, value):
    self._custom_operators = {} if value is None else value

  @property
  def custom_reducers(self):
    """:obj:`dict`: User-defined reducer functions for the reduce verb."""
    return self._custom_reducers

  @custom_reducers.setter
  def custom_reducers(self, value):
    self._custom_reducers = {} if value is None else value

  @property
  def track_types(self):
    """:obj:`bool`: Does the processor keep track of value types."""
    return self._track_types

  @track_types.setter
  def track_types(self, value):
    self._track_types = value

  @property
  def cache(self):
    """:obj:`dict`: Cache of data layers for the query execution."""
    return self._cache

  @cache.setter
  def cache(self, value):
    self._cache = value

  @property
  def preview(self):
    """:obj:`bool`: Is the query being processed in preview mode."""
    return self._preview

  @preview.setter
  def preview(self, value):
    self._preview = value

  @classmethod
  def parse(cls, recipe, datacube, mapping, space, time,
            spatial_resolution, crs = None, tz = None, **config):
    """Parse a semantic query.

    During query parsing, the required components for processing are read and
    converted all together into a single object which will be used internally
    for further processing of the query. Hence, query parsing takes care of
    initializing a :class:`QueryProcessor` instance. It also rasterizes the
    given spatial extent and combines it with the temporal extent into a single
    spatio-temporal array (see :func:`parse_extent`
    <semantique.processor.utils.parse_extent>`).

    Parameters
    ----------
      recipe : QueryRecipe
        The query recipe to be processed.
      datacube : Datacube
        The datacube instance to process the query against.
      mapping : Mapping
        The mapping instance to process the query against.
      space : SpatialExtent
        The spatial extent in which the query should be processed.
      time : TemporalExtent
        The temporal extent in which the query should be processed.
      spatial_resolution : :obj:`list`
        Spatial resolution in which the query should be processed. Should be
        given as a list in the format `[y, x]`, where y is the cell size along
        the y-axis, x is the cell size along the x-axis, and both are given as
        :obj:`int` or :obj:`float` value expressed in the units of the CRS.
        These values should include the direction of the axes. For most CRSs,
        the y-axis has a negative direction, and hence the cell size along the
        y-axis is given as a negative number.
      crs : optional
        Spatial coordinate reference system in which the query should be
        processed. Can be given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the provided spatial extent is used.
      tz : optional
        Time zone in which the query should be processed. Can be given as
        :obj:`str` referring to the name of a time zone in the tz database, or
        as instance of any class inheriting from :class:`datetime.tzinfo`. If
        :obj:`None`, the timezone of the provided temporal extent is used.
      **config:
        Additional configuration parameters forwarded to the initializer of the
        QueryProcessor instance. See :class:`QueryProcessor`.

    Returns
    -------
      :obj:`QueryProcessor`
        A query processor instance.

    Note
    -----
      Ideally, parsing should also take care of validating the components and
      their interrelations. For example, it should check if referenced concepts
      in the provided query recipe are actually defined in the provided
      mapping. Such functionality is not implemented yet.

    """
    # Step 0: Retrieve resolution for coarse-scale preview analyses
    if config.get("preview"):
      logger.info("--- Preview mode (reduced resolution) ---")
      output_shape = (5, 5)
      bounds = space.features.to_crs(crs).total_bounds
      x_res = (bounds[2] - bounds[0]) / output_shape[1]
      y_res = (bounds[3] - bounds[1]) / output_shape[0]
      spatial_resolution = [-y_res, x_res]
    # Step I: Parse the spatio-temporal extent.
    # It needs to be stored as a 2D array with dimensions space and time.
    logger.info("Started parsing the semantic query")
    extent = utils.parse_extent(
      spatial_extent = space,
      temporal_extent = time,
      spatial_resolution = spatial_resolution,
      crs = crs,
      tz = tz
    )
    logger.debug(f"Parsed the spatio-temporal extent:\n{extent}")
    # Step II: Initialize the QueryProcessor instance.
    out = cls(recipe, datacube, mapping, extent, **config)
    # Return.
    logger.info("Finished parsing the semantic query")
    return out

  def optimize(self):
    """Optimize a semantic query.

    During query optimization, the query components are scanned and certain
    properties of the query processor are set. These properties influence some
    tweaks in how the query processor will behave when processing the query.
    For example, if the given spatial extent consists of multiple dispersed
    sub-areas, the query processor might instruct itself to load data
    separately for each sub-area, instead of loading data for the full extent
    and then subset it afterwards.

    Returns
    -------
      :obj:`QueryProcessor`
        An updated query processor instance.

    Note
    -----
      In the current version of semantique, the optimization phase only exists
      as a placeholder, and no properties are updated yet.

    """
    logger.info("Started optimizing the semantic query")
    out = self
    logger.info("Finished optimizing the semantic query")
    return out

  def execute(self):
    """Execute a semantic query.

    During query execution, the query processor executes the result
    instructions of the query recipe. It solves all references, evaluates them
    into arrays, and applies the defined actions to them.

    Returns
    -------
      :obj:`QueryProcessor`
        An updated query processor instance, with a :attr:`response` property
        containing the resulting arrays.

    """
    logger.info("Started executing the semantic query")
    # Execute instructions for each result in the recipe.
    for x in self._recipe:
      if x in self._response:
        continue
      logger.info(f"Started executing result: '{x}'")
      result = self.call_handler(self._recipe[x])
      result.name = x
      self._response[x] = result
      logger.info(f"Finished executing result: '{x}'")
    # Post-process.
    out = self._response
    logger.info("Finished executing the semantic query")
    logger.debug(f"Responding:\n{out}")
    return out

  def call_handler(self, block, key = "type"):
    """Call the handler for a specific building block.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block.
      key : :obj:`str`
        The key that identifies the handler to be called.

    Returns
    --------
      :obj:`xarray.DataArray` or :obj:`Collection`
        The processed building block.

    Raises
    ------
      :obj:`exceptions.InvalidBuildingBlockError`
        If a handler for the provided building block cannot be found.

    """
    # Get handler.
    try:
      btype = block[key]
    except TypeError:
      raise exceptions.InvalidBuildingBlockError(
        "Block is not subscriptable"
      )
    except KeyError:
      raise exceptions.InvalidBuildingBlockError(
        f"Block has no '{key}' key"
      )
    try:
      handler = getattr(self, "handle_" + btype)
    except AttributeError:
      raise exceptions.InvalidBuildingBlockError(
        f"Unknown block type: '{btype}'"
      )
    # Call handler.
    return handler(block)

  def handle_concept(self, block):
    """Handler for semantic concept references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "concept".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    logger.debug(f"Translating concept {block['reference']}")
    out = self._mapping.translate(
      *block["reference"],
      property = block["property"] if "property" in block else None,
      extent = self._extent,
      datacube = self._datacube,
      eval_obj = self._get_eval_obj(),
      preview = self._preview,
      cache = self._cache,
      custom_verbs = self._custom_verbs,
      custom_operators = self._custom_operators,
      custom_reducers = self._custom_reducers,
      track_types = self._track_types
    )
    logger.debug(f"Translated concept {block['reference']}:\n{out}")
    return out

  def handle_layer(self, block):
    """Handler for data layer references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "layer".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    # Get data.
    layer_key = "_".join(block["reference"])
    if layer_key in self._cache.data:
      logger.debug(f"Loading layer {block['reference']} from cache")
      out = self._cache.load(layer_key)
    else:
      logger.debug(f"Retrieving layer {block['reference']}")
      out = self._datacube.retrieve(
        *block["reference"],
        extent = self._extent
      )
    logger.debug(f"Retrieved layer {block['reference']}:\n{out}")
    # Update cache
    self._cache.update(layer_key, out)
    logger.debug("Cache updated")
    logger.debug(f"Sequence of layers: {self._cache._seq}")
    logger.debug(f"Currently cached layers: {list(self._cache._data.keys())}")
    return out

  def handle_result(self, block):
    """Handler for result references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "result".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    Raises
    ------
      :obj:`exceptions.UnknownResultError`
        If the referenced result is not present in the query recipe.

    """
    name = block["name"]
    logger.debug(f"Fetching result '{name}'")
    # Process referenced result if it is not processed yet.
    if name not in self._response:
      try:
        instructions = self._recipe[name]
      except KeyError:
        raise exceptions.UnknownResultError(
          f"Recipe does not contain result '{name}'"
        )
      logger.info(f"Started executing result: '{name}'")
      result = self.call_handler(instructions)
      result.name = name
      self._response[name] = result
      logger.info(f"Finished executing result: '{name}'")
    # Return referenced result.
    out = self._response[name]
    logger.debug(f"Fetched result '{name}':\n{out}")
    return self._response[name]

  def handle_self(self, block):
    """Handler for self references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "self".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    out = self._get_eval_obj()
    logger.debug(f"Solved self reference:\n{out}")
    return out

  def handle_collection(self, block):
    """Handler for collection references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "collection".

    Returns
    -------
      :obj:`processor.arrays.Collection`

    """
    logger.debug("Constructing collection of arrays")
    out = [self.call_handler(x) for x in block["elements"]]
    out = arrays.Collection(out)
    logger.debug(f"Constructed collection of:\n{[x.name for x in out]}")
    return out

  def handle_processing_chain(self, block):
    """Handler for processing chains.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "processing_chain".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    obj = self.call_handler(block["with"])
    self._set_eval_obj(obj)
    for i in block["do"]:
      out = self.call_handler(i)
    self._reset_eval_obj()
    return out

  def handle_verb(self, block):
    """Handler for verbs.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    out = self.call_handler(block, key = "name")
    # Set the output array as the new active evaluation object.
    self._replace_eval_obj(out)
    return out

  def handle_evaluate(self, block):
    """Handler for the evaluate verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "evaluate".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Parse right hand side of expression.
    # If this is a reference it should be evaluated into an array.
    try:
      y = params["y"]
    except KeyError:
      pass
    else:
      try:
        params["y"] = self.call_handler(y)
      except exceptions.InvalidBuildingBlockError:
        pass
    # Obtain operator function.
    params["operator"] = self.get_operator(params["operator"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("evaluate", params)

  def handle_extract(self, block):
    """Handler for the extract verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "extract".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    return self.call_verb("extract", block["params"])

  def handle_filter(self, block):
    """Handler for the filter verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "filter".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Evaluate filterer reference into an array.
    params["filterer"] = self.call_handler(params["filterer"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("filter", params)

  def handle_assign(self, block):
    """Handler for the assign verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "assign".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Parse right hand side of expression.
    # If this is a reference it should be evaluated into an array.
    try:
      y = params["y"]
    except KeyError:
      pass
    else:
      try:
        params["y"] = self.call_handler(y)
      except exceptions.InvalidBuildingBlockError:
        pass
    # Parse at reference for conditional assignment.
    try:
      at = params["at"]
    except KeyError:
      pass
    else:
      params["at"] = self.call_handler(params["at"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("assign", params)

  def handle_groupby(self, block):
    """Handler for the groupby verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "groupby".

    Returns
    -------
      :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Evaluate grouper reference into an array or collection of arrays.
    params["grouper"] = self.call_handler(params["grouper"])
    # Call verb.
    return self.call_verb("groupby", params)

  def handle_reduce(self, block):
    """Handler for the reduce verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "reduce".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Obtain reducer function.
    params["reducer"] = self.get_reducer(params["reducer"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("reduce", params)

  def handle_shift(self, block):
    """Handler for the shift verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "shift".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    return self.call_verb("shift", block["params"])

  def handle_smooth(self, block):
    """Handler for the smooth verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "smooth".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Obtain reducer function.
    params["reducer"] = self.get_reducer(params["reducer"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("smooth", params)

  def handle_trim(self, block):
    """Handler for the trim verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "trim".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    return self.call_verb("trim", block["params"])

  def handle_delineate(self, block):
    """Handler for the delineate verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "delineate".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get and update function parameters.
    params = copy.deepcopy(block["params"])
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("delineate", block["params"])

  def handle_fill(self, block):
    """Handler for the fill verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "fill".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("fill", params)

  def handle_name(self, block):
    """Handler for the name verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "name".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    return self.call_verb("name", block["params"])

  def handle_apply_custom(self, block):
    """Handler for the apply_custom verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "apply_custom".

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Fetch the corresponding custom verb function.
    try:
      name = params["verb"]
      func = self._custom_verbs[name]
    except KeyError:
      raise exceptions.UnknownVerbError(
        f"Custom verb '{name}' is not defined"
      )
    params["verb"] = func
    # Set other function parameters.
    params["track_types"] = self._track_types
    return self.call_verb("apply_custom", params)

  def handle_compose(self, block):
    """Handler for the compose verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "compose".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    # Set function parameters.
    params = copy.deepcopy(block["params"])
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("compose", params)

  def handle_concatenate(self, block):
    """Handler for the concatenate verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "concatenate".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    # Set function parameters.
    params = copy.deepcopy(block["params"])
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("concatenate", params)

  def handle_merge(self, block):
    """Handler for the merge verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "merge".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Obtain reducer function.
    params["reducer"] = self.get_reducer(params["reducer"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("merge", params)

  def handle_label(self, block):
    """Handler for value labels.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "label".

    Returns
    -------
      :obj:`int` or :obj:`float`
        The value belonging to the label.

    Raises
    -------
      :obj:`exceptions.UnknownLabelError`
        If the label is not used for any value in the active evaluation object,
        or the active evaluation object has no value labels defined at all.

    """
    obj = self._get_eval_obj()
    label_mapping = obj.sq.value_labels
    if label_mapping is None:
      raise exceptions.UnknownLabelError(
        "Active evaluation object has no value labels defined"
      )
    label = block["content"]
    out = None # Initialize.
    for i, x in enumerate(label_mapping.values()):
      if x == label:
        out = list(label_mapping.keys())[i]
        break
    if out is None:
      raise exceptions.UnknownLabelError(
        f"There is no value with label '{label}'"
      )
    logger.debug(f"Matched label '{label}' with index {out}")
    return out

  def handle_set(self, block):
    """Handler for value sets.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "set".

    Returns
    -------
      :obj:`list`

    """
    values = block["content"]
    def _update(x):
      try:
        return self.call_handler(x)
      except exceptions.InvalidBuildingBlockError:
        return x
    out = [_update(x) for x in values]
    return out

  def handle_interval(self, block):
    """Handler for value intervals.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "interval".

    Returns
    -------
      :obj:`Interval <semantique.processor.values.Interval>`

    """
    lower = block["content"][0]
    upper = block["content"][1]
    out = values.Interval(lower, upper)
    logger.debug(f"Parsed interval::\n{out}")
    return out

  def handle_geometry(self, block):
    """Handler for spatial vector geometries.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "geometry".

    Returns
    -------
      :obj:`geopandas.GeoDataFrame`

    """
    feats = block["content"]["features"]
    crs = pyproj.CRS.from_string(block["content"]["crs"])
    out = gpd.GeoDataFrame.from_features(feats, crs = crs)
    if crs != self.crs:
      out = out.to_crs(self.crs)
    logger.debug(f"Parsed geometry:\n{out}")
    return out

  def handle_time_instant(self, block):
    """Handler for time instants.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "time_instant".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    dt = np.datetime64(block["content"]["start"], "ns")
    tz = pytz.timezone(block["content"]["tz"])
    if tz != self.tz.zone:
      dt = utils.convert_datetime64(dt, tz, self.tz)
    out = np.array([dt])
    logger.debug(f"Parsed time instant:\n{out}")
    return out

  def handle_time_interval(self, block):
    """Handler for time intervals.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "time_interval".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    start = np.datetime64(block["content"]["start"], "ns")
    end = np.datetime64(block["content"]["end"], "ns")
    tz = pytz.timezone(block["content"]["tz"])
    if tz != self.tz:
      start = utils.convert_datetime64(start, tz, self.tz)
      end = utils.convert_datetime64(start, tz, self.tz)
    out = np.array([start, end])
    logger.debug(f"Parsed time interval:\n{out}")
    return out

  def add_custom_verb(self, name, function):
    """Add a new verb to the set of user-defined verbs.

    Parameters
    ----------
      name : :obj:`str`
        Name of the verb to be added.
      function : :obj:`callable`
        Operator function to be added.

    """
    new = {name: function}
    self._custom_verbs.update(new)

  def call_verb(self, name, params):
    """Apply a verb to the active evaluation object.

    Parameters
    -----------
      name : :obj:`str`
        Name of the verb.
      params : :obj:`dict`
        Parameters to be forwarded to the verb.

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`

    """
    # Get the object to apply the verb to.
    obj = self._get_eval_obj()
    # Apply the verb.
    verb = getattr(obj.sq, name)
    out = verb(**params)
    # Warn when output array is empty.
    try:
      is_empty = out.sq.is_empty
    except AttributeError:
      is_empty = out.is_empty
    if is_empty:
      warnings.warn(
        f"Verb '{name}' returned an empty array"
      )
    logger.debug(f"Applied verb {name}:\n{out}")
    return out

  def add_custom_operator(self, name, function):
    """Add a new operator to the set of user-defined operators.

    Parameters
    ----------
      name : :obj:`str`
        Name of the operator to be added.
      function : :obj:`callable`
        Operator function to be added.

    """
    new = {name: function}
    self._custom_operators.update(new)

  def get_operator(self, name):
    """Obtain a supported operator function.

    Parameters
    ----------
      name : :obj:`str`
        Name of the operator function.

    Returns
    --------
      :obj:`callable`
        The operator function corresponding to the given name.

    Raises
    ------
      :obj:`exceptions.UnknownOperatorError`
        If an operator function corresponding to the given name cannot be
        found.

    """
    # First try to get the operator from the dict of user-defined operators.
    # Then try to get it from the module with built-in semantique operators.
    try:
      func = self._custom_operators[name]
    except KeyError:
      try:
        func = getattr(operators, f"{name}_")
      except AttributeError:
        raise exceptions.UnknownOperatorError(
          f"Operator '{name}' is not defined"
        )
    return func

  def add_custom_reducer(self, name, function):
    """Add a new reducer to the set of user-defined reducers.

    Parameters
    ----------
      name : :obj:`str`
        Name of the reducer to be added.
      function : :obj:`callable`
        Reducer function to be added.

    """
    new = {name: function}
    self._custom_reducers.update(new)

  def get_reducer(self, name):
    """Obtain a supported reducer function.

    Parameters
    ----------
      name : :obj:`str`
        Name of the reducer function.

    Returns
    --------
      :obj:`callable`
        The reducer function corresponding to the given name.

    Raises
    ------
      :obj:`exceptions.UnknownReducerError`
        If an reducer function corresponding to the given name cannot be
        found.

    """
    # First try to get the reducer from the dict of user-defined reducers.
    # Then try to get it from the module with built-in semantique reducers.
    try:
      func = self._custom_reducers[name]
    except KeyError:
      try:
        func = getattr(reducers, f"{name}_")
      except AttributeError:
        raise exceptions.UnknownReducerError(
          f"Reducer '{name}' is not defined"
        )
    return func

  def _get_eval_obj(self):
    return self._eval_obj[-1]

  def _replace_eval_obj(self, obj):
    self._eval_obj[-1] = obj

  def _reset_eval_obj(self):
    del self._eval_obj[-1]

  def _set_eval_obj(self, obj):
    self._eval_obj.append(obj)

class FakeProcessor(QueryProcessor):
  """
  Worker that simulates the processing of a semantic query recipe.
  It doesn't actually process the query, but can be used to translate concepts
  and retrieve data layer names.

  Parameters
  ----------
    recipe : QueryRecipe
      The query recipe to be processed.
    datacube : Datacube
      The datacube instance to process the query against.
    mapping : Mapping
      The mapping instance to process the query against.
    extent : :obj:`xarray.DataArray`
      The spatio-temporal extent in which the query should be processed. Should
      be given as an array with a temporal dimension and two spatial dimensions
      such as returned by
      :func:`parse_extent <semantique.processor.utils.parse_extent>`.
    custom_verbs : :obj:`dict`, optional
      User-defined verbs that may be used when executing the query recipe in
      addition to the built-in verbs in semantique.
    custom_operators : :obj:`dict`, optional
      User-defined operator functions that may be used when evaluating
      expressions with the evaluate verb in addition to the built-in operators
      in semantique. Built-in operators with the same name will be overwritten.
    custom_reducers : :obj:`dict`, optional
      User-defined reducer functions that may be used when reducing array
      dimensions with the reduce verb in addition to the built-in reducers in
      semantique. Built-in reducers with the same name will be overwritten.
    track_types : :obj:`bool`
      Should the query processor keep track of the value type of arrays
      when applying processes, and promote them if necessary? This option is
      always disabled for the FakeProcessor since it doesn't evaualte processes
      and therefore can't check the validity of the types of the arrays.
    preview : :obj:`bool`
      Run the query processor with reduced resolution to test the recipe execution.
      Preview-runs are necessary if cache should be used.
    cache : :obj:`Cache`
      The cache object that is used to store data layers.
  """
  def __init__(self, recipe, datacube, mapping, extent, custom_verbs = None,
               custom_operators = None, custom_reducers = None,
               track_types = True, preview = False, cache = None):
    super(FakeProcessor, self).__init__(
      recipe, datacube, mapping, extent, custom_verbs=custom_verbs,
      custom_operators=custom_operators, custom_reducers=custom_reducers,
      track_types=track_types, preview=preview, cache=cache
      )
    self.track_types = False

  def call_verb(self, name, params):
    """Apply a verb to the active evaluation object.

    Parameters
    -----------
      name : :obj:`str`
        Name of the verb.
      params : :obj:`dict`
        Parameters to be forwarded to the verb.

    Returns
    -------
      :obj:`xarray.DataArray` or :obj:`Collection <semantique.processor.arrays.Collection>`
    """
    return self._get_eval_obj()

  def handle_concept(self, block):
    """Handler for semantic concept references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "concept".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    logger.debug(f"Translating concept {block['reference']}")
    out = self._mapping.translate(
      *block["reference"],
      property = block["property"] if "property" in block else None,
      extent = self._extent,
      datacube = self._datacube,
      eval_obj = self._get_eval_obj(),
      processor = FakeProcessor,
      preview = self._preview,
      cache = self._cache,
      custom_verbs = self._custom_verbs,
      custom_operators = self._custom_operators,
      custom_reducers = self._custom_reducers,
      track_types = self._track_types
    )
    logger.debug(f"Translated concept {block['reference']}:\n{out}")
    return out

  def handle_label(self, block):
    """Handler for value labels.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "label".

    Returns
    -------
      :obj:None
    """
    return None

  def handle_layer(self, block):
    """Handler for data layer references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "layer".

    Returns
    -------
      :obj:`xarray.DataArray`
    """
    self._cache.build(block["reference"])
    return xr.full_like(self._extent, np.nan)

class FilterProcessor(QueryProcessor):
  """
  Worker that processes a semantic query recipe to retrieve the required temporal
  extent of all layers that are used in the recipe.

  Parameters
  ----------
    recipe : QueryRecipe
      The query recipe to be processed.
    datacube : Datacube
      The datacube instance to process the query against.
    mapping : Mapping
      The mapping instance to process the query against.
    extent : :obj:`xarray.DataArray`
      The spatio-temporal extent in which the query should be processed. Should
      be given as an array with a temporal dimension and two spatial dimensions
      such as returned by
      :func:`parse_extent <semantique.processor.utils.parse_extent>`.
    custom_verbs : :obj:`dict`, optional
      User-defined verbs that may be used when executing the query recipe in
      addition to the built-in verbs in semantique.
    custom_operators : :obj:`dict`, optional
      User-defined operator functions that may be used when evaluating
      expressions with the evaluate verb in addition to the built-in operators
      in semantique. Built-in operators with the same name will be overwritten.
    custom_reducers : :obj:`dict`, optional
      User-defined reducer functions that may be used when reducing array
      dimensions with the reduce verb in addition to the built-in reducers in
      semantique. Built-in reducers with the same name will be overwritten.
    track_types : :obj:`bool`
      Should the query processor keep track of the value type of arrays
      when applying processes, and promote them if necessary? This option is
      always disabled for the FakeProcessor since it doesn't evaualte processes
      and therefore can't check the validity of the types of the arrays.
    preview : :obj:`bool`
      Run the query processor with reduced resolution to test the recipe execution.
      Preview-runs are necessary if cache should be used.
    cache : :obj:`Cache`
      The cache object that is used to store data layers.
    watch_layer : :obj:`str`
      The layer that is currently being focussed on. All temporal filter operations
      are analyzed regarding the effect they are having on this layer. Doesn't need
      to be set by the user but will be set dynamically upon calling .execute().
    meta_timestamps : :obj:`DatetimeIndex` or :obj:`Series`
      The timestamps that are used as a starting point to determine the final temporal
      extent. The timestamps will be filtered by the temporal filters. If left to `None`,
      the timestamps will be retrieved dynamically from the referenced data layers upon
      calling .execute(). Only for test purposes meta_timestamps should be provided by
      the user.
  """
  def __init__(self, recipe, datacube, mapping, extent, custom_verbs = None,
               custom_operators = None, custom_reducers = None, track_types = True,
               preview = False, cache = None, watch_layer = None,
               meta_timestamps = None):
    super(FilterProcessor, self).__init__(
      recipe, datacube, mapping, extent, custom_verbs=custom_verbs,
      custom_operators=custom_operators, custom_reducers=custom_reducers,
      track_types=track_types, preview=preview, cache=cache
      )
    self.track_types = False
    self.watch_layer = watch_layer
    self.meta_timestamps = meta_timestamps

  @property
  def crs(self):
    """:obj:`pyproj.crs.CRS`: Spatial coordinate reference system in which the
    query should be processed."""
    return self._extent.sqm.crs

  @property
  def spatial_resolution(self):
    """:obj:`list`: Spatial resolution in which the query should be
    processed."""
    return self._extent.sqm.spatial_resolution

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Time zone in which the query should be
    processed."""
    return self._extent.sqm.tz

  @property
  def watch_layer(self):
    """:obj:`str`: The layer that is currently being focussed on."""
    return self._watch_layer

  @watch_layer.setter
  def watch_layer(self, value):
    self._watch_layer = value

  @property
  def meta_timestamps(self):
    """:obj:`DatetimeIndex` or :obj:`Series`: The timestamps that are used as
    a starting point to determine the final temporal extent."""
    return self._meta_timestamps

  @meta_timestamps.setter
  def meta_timestamps(self, value):
    self._meta_timestamps = value

  def execute(self):
    """Execute a semantic query.

    During query execution, the query processor executes the result
    instructions of the query recipe. It solves all references, evaluates them
    into arrays, and applies the defined actions to them.

    Returns
    -------
      :obj:`FilterProcessor`
        An updated filter processor instance, with a :attr:`response` property
        containing the resulting timestamps that are processed in the recipe.

    """
    logger.info("Started query processing for temporal filter evaluation.")

    # Step 1: Run fake processor to get a dict of all layers.
    logger.info("Started fake query processing to resolve layer references.")
    self.fap = FakeProcessor(
      recipe=self.recipe,
      datacube=self.datacube,
      mapping=self.mapping,
      extent=self.extent,
      track_types=self.track_types,
      custom_verbs=self.custom_verbs,
      custom_operators=self.custom_operators,
      custom_reducers=self.custom_reducers,
      preview=self.preview,
      cache=self.cache
    )
    _ = self.fap.optimize().execute()
    lyrs = [list(x) for x in set(tuple(x) for x in self.fap.cache.seq)]
    self._response = {"_".join(x): {} for x in lyrs}
    logger.info("Finished fake query processing to resolve layer references.")
    logger.debug(f"Resolved layers: {lyrs}")

    # Step 2.a: Temporal filter evaluation.
    concepts = self._find_and_resolve_concepts(self.recipe)
    if any([self._contains_filter(x) for x in [*concepts, self.recipe]]):
      skip_filter = False
      try:
        # Step 2.1.1: Retrieve timestamps of data layers to be filtered.
        if self.meta_timestamps is None:
          meta_retrieved = True
          if type(self.datacube) == datacube.Opendatacube:
            # Retrieve products to look up.
            lyr_prods_lut = {}
            for ref in self.fap.cache.seq:
              lyr_prods_lut[ref] = self.datacube.lookup(*ref)["product"]
            # Retrieve timestamps of prods.
            meta_dfs = []
            prods = list(set(lyr_prods_lut.values()))
            for prod in prods:
              ref_idx = list(lyr_prods_lut.values()).index(prod)
              lyr = list(lyr_prods_lut.keys())[ref_idx]
              df = self.datacube.retrieve_metadata(*lyr, extent=self._extent)
              df.insert(0, "prod", prod)
              meta_dfs.append(df)
            _meta_df = pd.concat(meta_dfs).reset_index(drop=True)
            # Expand meta df for lyr information.
            meta_dfs = []
            for lyr,prod in lyr_prods_lut.items():
              df = _meta_df[_meta_df['prod'] == prod].copy()
              df.insert(0, "lyr", "_".join(lyr))
              meta_dfs.append(df)
              logger.debug(f"Retrieved meta information for layer {lyr}:\n {df}")
              logger.debug(f"Unique timestamps: {len(df.drop_duplicates(['time']))}")
            meta_df = pd.concat(meta_dfs).reset_index(drop=True)
            meta_df.drop(columns=["prod"], inplace=True)
          elif type(self.datacube) == datacube.STACCube:
            # Retrieve timestamps of references.
            # Note: Contrary to ODC, .retrieve_metadata() can be called immediately
            # since the metadata is already stored in the STACCube. Hence, no performance
            # advantage by resolving products (=items instead of assets) first.
            meta_dfs = []
            for lyr in list(set(self.fap.cache.seq)):
              df = self.datacube.retrieve_metadata(*lyr, extent=self._extent)
              df.insert(0, "lyr", "_".join(lyr))
              meta_dfs.append(df)
              logger.debug(f"Retrieved meta information for layer {lyr}:\n {df}")
              logger.debug(f"Unique timestamps: {len(df.drop_duplicates(['time']))}")
            meta_df = pd.concat(meta_dfs).reset_index(drop=True)
          elif type(self.datacube) == datacube.GeotiffArchive:
            meta_retrieved = False
            raise ValueError("FilterProcessor doesn't support GeotiffArchive.")
          else:
            meta_retrieved = False
            raise ValueError(f"Datacube type {self.datacube} not supported.")
          # Set meta timestamps to be analysed.
          self.meta_timestamps = meta_df.time.unique()
        else:
          meta_retrieved = False

        if len(self.meta_timestamps):
          # Step 2.1.2: Execute instructions for each layer & result in the recipe.
          for lyr in lyrs:
            self.watch_layer = "_".join(lyr)
            logger.info(f"Evaluate temporal filter for layer: '{lyr}'")
            for x in self._recipe:
              logger.info(f"Started executing result: '{x}'")
              result = self.call_handler(self._recipe[x])
              result.name = x
              self._response[self.watch_layer][x] = result
              logger.info(f"Finished executing result: '{x}'")

          # Step 2.1.3: Postprocessing of results.
          # Omit non-active results.
          for lyr,arr_dict in self._response.items():
            self._response[lyr] = {
              k: v for k, v in arr_dict.items() if v.sqm.active
            }
          # Combine temporal extents of Collections.
          # Arrive at set of arrays as results.
          for lyr,arr_dict in self._response.items():
            for k,v in arr_dict.items():
              if type(v.sqm).__name__ == 'MetaCollection':
                # Fill datetime arrays with ones.
                for i,arr in enumerate(v):
                  if np.issubdtype(arr.dtype, np.datetime64):
                    v[i] = xr.ones_like(arr, dtype="int32")
              if type(v.sqm).__name__ == 'MetaCollection':
                self._response[lyr][k] = v.sqm.merge(reducers.any_, track_types=False)
          # Retrieve valid temporal indices per layer and result.
          # Valid indices are those that are not null.
          response = copy.deepcopy(self._response)
          for lyr,arr_dict in self._response.items():
            for res,arr in arr_dict.items():
              # Create empty response obj.
              out = []
              # Reduce arrays to at most 3 dimensions.
              if arr.ndim > 3:
                dim = [x for x in arr.dims if x not in ["time", "x", "y"]]
                assert len(dim) == 1, "Only one dimension can be reduced"
                arr = arr.sqm.reduce(reducers.any_, dim[0], track_types=False)
              # Extract temporal indices from array itself.
              if "time" in arr.dims:
                reduce_dims = [x for x in arr.dims if x != "time"]
                out.append(arr.time[arr.isnull().sum(reduce_dims) == 0])
              # Extract saved results from vault.
              if arr.sqm.vault is not None:
                out.append(arr.sqm.vault)
              # Combine results from array and vault.
              if len(out):
                if len(out) > 1:
                  out = xr.DataArray(np.unique(np.concatenate(out)), dims="time", name="time")
                else:
                  out = out[0]
                response[lyr][res] = out
              else:
                # If no valid temporal indices are found, remove result.
                # Occurs if result are extracted spatial coordinates.
                response[lyr].pop(res)
          self._response = response
          # Omit empty layer results.
          self._response = {k:v for k,v in self._response.items() if v}
          if len(self._response):
            # Create temporal extents' union over results for each layer.
            for lyr,arr_dict in self._response.items():
              time_coords = [arr_dict[key].values for key in arr_dict]
              merged_time = np.unique(np.concatenate(time_coords))
              merged_time.sort()
              self._response[lyr] = pd.to_datetime(merged_time)
            # Sort response items.
            self._response = {k: v for k, v in sorted(self._response.items())}
          else:
            skip_filter = True
      except Exception as e:
        skip_filter = True
        logger.error(f"An error occurred during FilterProcessor execution: {e}")
        logger.error("FilterProcessor evaluation is skipped.")
    else:
      skip_filter = True
      meta_retrieved = False

    # Step 2.b: Shortcut if no temporal filter is present.
    # Keep all initial timestamps.
    if skip_filter:
      for lyr in lyrs:
        self._response["_".join(lyr)] = pd.to_datetime(self.meta_timestamps)
    self._response = {k: v for k, v in sorted(self._response.items())}

    # Step 3: Update datacube dataset according to valid timestamps.
    if meta_retrieved:
      # Copy datacube to update.
      if type(self.datacube) == datacube.Opendatacube:
        # Copy datacube.
        dc_con = self.datacube.connection
        self.datacube.connection = None
        _datacube = copy.deepcopy(self.datacube)
        _datacube.connection = dc_con
        self.datacube.connection = dc_con
        # Extract valid dataset ids corresponding to timestamps.
        id_dict = {}
        for k,v in self._response.items():
          ids = meta_df[meta_df.lyr == k][meta_df.time.isin(pd.Series(v))].id
          id_dict[k] = list(ids)
          logger.debug(f"Temporally filtered results for layer {k}")
          logger.debug(f"- unique timestamps: {len(np.unique(v))}")
          logger.debug(f"- unique datasets: {len(ids)}")
        _datacube.data_dict = id_dict
        self.datacube = _datacube
      elif type(self.datacube) == datacube.STACCube:
        # Copy datacube.
        _datacube = copy.deepcopy(self.datacube)
        _datacube.src = pystac.ItemCollection(self.datacube.src)
        # Extract valid collection_item_Ids corresponding to timestamps.
        id_list = []
        for k,v in self._response.items():
          ids = meta_df[meta_df.lyr == k][meta_df.time.isin(pd.Series(v))].id
          id_list.extend(list(ids))
          logger.debug(f"Temporally filtered results for layer {k}")
          logger.debug(f"- unique timestamps: {len(np.unique(v))}")
          logger.debug(f"- unique items: {len(ids)}")
        id_list = list(set(id_list))
        # Subset item as input to datacube correspondigly.
        if len(id_list):
          filtered_items = []
          for item in _datacube.src:
            if (item.get_collection().id, item.id) in id_list:
              filtered_items.append(item)
          _datacube.src = filtered_items
        else:
          _datacube.src = []
        self.datacube = _datacube

    # Step 4: Return result.
    out = self._response
    logger.info("Finished query processing for temporal filter evaluation.")
    return out

  def call_verb(self, name, params):
    """Apply a verb to the active evaluation object.

    Parameters
    -----------
      name : :obj:`str`
        Name of the verb.
      params : :obj:`dict`
        Parameters to be forwarded to the verb.

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`MetaCollection <processor.arrays.MetaCollection>`

    """
    # Get the object to apply the verb to.
    obj = self._get_eval_obj()
    # Apply the verb.
    verb = getattr(obj.sqm, name)
    out = verb(**params)
    # Warn when output array is empty.
    try:
      is_empty = out.sqm.is_empty
    except AttributeError:
      is_empty = out.is_empty
    if is_empty:
      warnings.warn(
        f"Verb '{name}' returned an empty array"
      )
    logger.debug(f"Applied verb {name}:\n{out}")
    return out


  def handle_concept(self, block):
    """Handler for semantic concept references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "concept".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    logger.debug(f"Translating concept {block['reference']}")
    out = self._mapping.translate(
      *block["reference"],
      property = block["property"] if "property" in block else None,
      extent = self._extent,
      datacube = self._datacube,
      eval_obj = self._get_eval_obj(),
      processor = FilterProcessor,
      preview = self._preview,
      cache = self._cache,
      custom_verbs = self._custom_verbs,
      custom_operators = self._custom_operators,
      custom_reducers = self._custom_reducers,
      track_types = self._track_types,
      watch_layer = self._watch_layer,
      meta_timestamps = self._meta_timestamps,
    )
    logger.debug(f"Translated concept {block['reference']}:\n{out}")
    return out

  def handle_collection(self, block):
    """Handler for collection references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "collection".

    Returns
    -------
      :obj:`processor.arrays.MetaCollection`

    """
    logger.debug("Constructing collection of arrays")
    list_obj = [self.call_handler(x) for x in block["elements"]]
    out = arrays.MetaCollection(list_obj)
    logger.debug(f"Constructed collection of:\n{[x.name for x in out]}")
    return out

  def handle_label(self, block):
    """Handler for value labels.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "label".

    Returns
    -------
      :obj:None
    """
    return None

  def handle_layer(self, block):
    """Handler for data layer references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "layer".

    Returns
    -------
      :obj:`xarray.DataArray`
    """
    self._cache.build(block["reference"])
    # Create a data array with the same shape as temporal extent.
    x_coords = [0, 1]
    y_coords = [0, 1]
    data = np.ones((len(self.meta_timestamps), len(y_coords), len(x_coords)))
    data_array = xr.DataArray(
      data,
      coords = {
        TIME: self.meta_timestamps,
        X: y_coords,
        Y: x_coords,
        "spatial_feats": ((Y, X), np.ones((len(y_coords), len(x_coords))))
      },
      dims = [TIME, Y, X]
    )
    # Set relevant data array properties.
    layer_key = "_".join(block["reference"])
    data_array.name = layer_key
    if layer_key == self._watch_layer:
      data_array.sqm.active = True
    else:
      data_array.sqm.active = False
    data_array.sqm.locked = False
    return data_array

  def handle_result(self, block):
    """Handler for result references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "result".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`MetaCollection <semantique.processor.arrays.MetaCollection>`

    Raises
    ------
      :obj:`exceptions.UnknownResultError`
        If the referenced result is not present in the query recipe.

    """
    name = block["name"]
    logger.debug(f"Fetching result '{name}'")
    # Process referenced result if it is not processed yet.
    if name not in self._response[self.watch_layer]:
      try:
        instructions = self._recipe[name]
      except KeyError:
        raise exceptions.UnknownResultError(
          f"Recipe does not contain result '{name}'"
        )
      logger.info(f"Started executing result: '{name}'")
      result = self.call_handler(instructions)
      result.name = name
      self._response[self.watch_layer][name] = result
      logger.info(f"Finished executing result: '{name}'")
    # Return referenced result.
    out = self._response[self.watch_layer][name]
    logger.debug(f"Fetched result '{name}':\n{out}")
    return out

  def _contains_filter(self, params):
    """Recursively check for temporal filter in nested dictionaries or lists.

    Parameters
    ----------
      params : :obj:`dict` or :obj:`list`
        The parameters to be checked.

    Returns
    -------
      :obj:`bool`
        True if temporal filter is found in the parameters, False otherwise.
    """
    if isinstance(params, dict):
      if params.get('type') == 'verb' and params.get('name') == 'filter':
        if 'params' in params:
          return self._contains_extract_time(params['params'])
      for key, value in params.items():
        if isinstance(value, (dict, list)):
          if self._contains_filter(value):
            return True
    elif isinstance(params, list):
      for item in params:
        if self._contains_filter(item):
          return True
    return False

  def _contains_extract_time(self, params):
    """Recursively check for 'extract time' in nested dictionaries or lists.

    Parameters
    ----------
      params : :obj:`dict` or :obj:`list`
        The parameters to be checked.

    Returns
    -------
      :obj:`bool`
        True if 'extract time' is found in the parameters, False otherwise.
    """
    if isinstance(params, dict):
      if params.get('type') == 'verb' and params.get('name') == 'extract':
        if 'params' in params and params['params'].get('dimension') == 'time':
          return True
      for key, value in params.items():
        if isinstance(value, (dict, list)):
          if self._contains_extract_time(value):
            return True
    elif isinstance(params, list):
      for item in params:
        if self._contains_extract_time(item):
          return True
    return False

  def _find_and_resolve_concepts(self, recipe):
    """Searches recursively for and resolve 'concept' references in a recipe.

    Parameters
    ----------
      recipe : :obj:`dict` or :obj:`list`
        The recipe (or pieces of it) which is to be checked for references.

    Returns
    -------
      :obj:`list`
        List of concept defitions that are referenced in the recipe.
    """
    results = []
    if isinstance(recipe, dict):
      # Check if this dict contains a concept reference
      if (
        recipe.get('type') == 'concept' and
        isinstance(recipe.get('reference'), tuple) and
        recipe['reference'][0] == 'entity'
      ):
        # Resolve reference
        ref = recipe['reference']
        property = recipe.get('property')
        ruleset = self._mapping.lookup(*ref)
        if property is None:
          results.append(ruleset)
        else:
          try:
            results.append(ruleset[property])
          except KeyError:
            raise KeyError(f"Property '{property}' is not defined for concept '{ref}'")
      else:
        # Recur into the dictionary to search for nested occurrences
        for key, value in recipe.items():
          results.extend(self._find_and_resolve_concepts(value))
    elif isinstance(recipe, list):
      # Recur into lists to search for nested dictionaries
      for item in recipe:
        results.extend(self._find_and_resolve_concepts(item))
    return results

class Cache:
  """Cache of retrieved data layers.

  The cache takes care of tracking the data references in their order of
  evaluation and retaining data layers in RAM if they are still needed for
  the further execution of the semantic query.
  """

  def __init__(self):
    self._seq = []
    self._data = {}

  @property
  def seq(self):
    """list: Sequence of references."""
    return self._seq

  @property
  def data(self):
    """dict: Data stored in the cache."""
    return self._data

  def build(self, ref):
    """Build of the sequence of data references."""
    self._add_to_seq(ref)

  def load(self, key):
    """Load data layer from cache."""
    return self._data.get(key, None)

  def update(self, key, data):
    """Modify cache content during evaluation."""
    if len(self._seq):
      current = self._seq[0]
      self._rm_from_seq(0)
      if current in self._seq:
        self._add_data(key, data)
      else:
        if key in self._data:
          self._rm_data(key)

  def _add_to_seq(self, ref):
    """Update sequence of data references."""
    self._seq.append(ref)

  def _rm_from_seq(self, idx):
    """Update sequence of data references."""
    del self._seq[idx]

  def _add_data(self, key, value):
    """Add data layer to cache."""
    self._data[key] = value

  def _rm_data(self, key):
    """Remove data layer from cache."""
    del self._data[key]
