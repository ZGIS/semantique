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
  """Worker that takes care of processing a semantic query.

  Parameters
  ----------
    recipe : QueryRecipe
      The query recipe to be processed.
    factbase : Factbase
      The factbase instance to process the query against.
    ontology : Ontology
      The ontology instance to process the query against.
    extent : :obj:`xarray.DataArray`
      The spatio-temporal extent in which the query should be processed. Should
      be given as an array with a temporal dimension as well as a stacked
      spatial dimension, such as returned by
      :func:`create_extent_cube <semantique.processor.utils.create_extent_cube>`.
    operators : :obj:`dict`
      Operator functions that may be used when evaluating expressions with the
      evaluate verb. If :obj:`None`, all built-in operators in semantique will
      be provided automatically.
    extra_operators : :obj:`dict`, optional
      Operator functions that may be used when evaluating expressions with the
      evaluate verb *in addition* to the built-in operators in semantique.
    reducers : :obj:`dict`
      Reducer functions that may be used when reducing data cube dimensions
      with the reduce verb. If :obj:`None`, all built-in reducers in semantique
      will be provided automatically.
    extra_reducers : :obj:`dict`, optional
      Reducer functions that may be used when reducing data cube dimensions
      with the reduce verb *in addition* to the built-in reducers in
      semantique.
    track_types : :obj:`bool`
      Should the query processor keep track of the value type of data cubes
      when applying processes, and promote them if necessary? Keeping track of
      value types also means throwing errors whenever a value type is not
      supported by a specific process.
    trim_filter : :obj:`bool`
      Should data cubes be trimmed after a filter verb is applied to them?
      Trimming means that all coordinates for which all values are nodata, are
      dropped from the array. The spatial dimension (if present) is treated
      differently, by trimming it only at the edges, and thus maintaining the
      regularity of the spatial dimension.
    trim_results : :obj:`bool`
      Should result data cubes be trimmed before returning the response?
      Trimming means that all coordinates for which all values are nodata, are
      dropped from the array. The spatial dimension (if present) is treated
      differently, by trimming it only at the edges, and thus maintaining the
      regularity of the spatial dimension.
    unstack_results : :obj:`bool`
      Should the spatial dimension (if present) in result data cubes be
      unstacked into respectively the separate x and y dimensions before
      returning the response?

  """

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
  def factbase(self):
    """Factbase: The factbase instance to process the query
    against."""
    return self._factbase

  @factbase.setter
  def factbase(self, value):
    self._factbase = value

  @property
  def ontology(self):
    """Ontology: The ontology instance to process the query
    against."""
    return self._ontology

  @ontology.setter
  def ontology(self, value):
    self._ontology = value

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
  def operators(self):
    """:obj:`dict`: Supported operator functions for the evaluate verb."""
    return self._operators

  @operators.setter
  def operators(self, value):
    self._operators = value

  @property
  def reducers(self):
    """:obj:`dict`: Supported reducer functions for the reduce verb."""
    return self._reducers

  @reducers.setter
  def reducers(self, value):
    self._reducers = value

  @property
  def track_types(self):
    """:obj:`bool`: Does the processor keep track of value types."""
    return self._track_types

  @track_types.setter
  def track_types(self, value):
    self._track_types = value

  @property
  def trim_filter(self):
    """:obj:`bool`: Are arrays always trimmed after filtering."""
    return self._trim_filter

  @trim_filter.setter
  def trim_filter(self, value):
    self._trim_filter = value

  @property
  def trim_results(self):
    """:obj:`bool`: Are result arrays trimmed before returning."""
    return self._trim_results

  @trim_results.setter
  def trim_results(self, value):
    self._trim_results = value

  @property
  def unstack_results(self):
    """:obj:`bool`: Are result arrays unstacked before returning."""
    return self._unstack_results

  @unstack_results.setter
  def unstack_results(self, value):
    self._unstack_results = value

  @classmethod
  def parse(cls, recipe, factbase, ontology, space, time,
            spatial_resolution, crs = None, tz = None, **config):
    """Parse a semantic query.

    During query parsing, the required components for processing are read and
    converted all together into a single query processor object which will be
    used further process the query. Parsing should take care of validating the
    components and their interrelations. A specific task of the query parser in
    semantique is also to combine the spatial and temporal extent of the query
    into a single spatio-temporal extent cube.

    Parameters
    ----------
      recipe : QueryRecipe
        The query recipe to be processed.
      factbase : Factbase
        The factbase instance to process the query against.
      ontology : Ontology
        The ontology instance to process the query against.
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
      The current implementation of query parsing is still very basic in its
      functionalities. It initializes the query processor object, but does not
      validate the query components yet.

    """
    # Step I: Parse the spatio-temporal extent.
    # It needs to be stored as a 2D array with dimensions space and time.
    extent = utils.create_extent_cube(
      spatial_extent = space,
      temporal_extent = time,
      spatial_resolution = spatial_resolution,
      crs = crs,
      tz = tz
    )
    # Step II: Initialize the QueryProcessor instance.
    return cls(recipe, factbase, ontology, extent, **config)

  def optimize(self):
    """Optimize a semantic query.

    During query optimization, the query processor is scanned and an execution
    plan is written. The execution plan is a step-by-step guide of how the
    query should be executed during the execution phase that follows. Creating
    this causes some overhead, but that should be balanced out by considerably
    faster execution times.

    Returns
    -------
      :obj:`QueryProcessor`
        An updated query processor instance.

    Note
    -----
      The current implementation of query parsing is still very basic in its
      functionalities. To be honest, it currently does exactly nothing. That
      is, the query will be executed ‘as-is’, and not yet according to an
      optimized query execution plan. The function is merely provided as a
      placeholder for a future implementation of query optimization.

    """
    return self

  def execute(self):
    """Execute a semantic query.

    During query execution, the query processor follows the execution plan and
    executes all result instructions.

    Returns
    -------
      :obj:`QueryProcessor`
        An updated query processor instance, with a :attr:`response` property
        containing the resulting data cubes.

    """
    for x in self._recipe:
      if x in self._response:
        continue
      result = self.call_handler(self._recipe[x])
      result.name = x
      self._response[x] = result
    return self

  def respond(self):
    """Return the response of an executed semantic query.

    Returns
    -------
      :obj:`dict` of :obj:`xarray.DataArray`
        Dictionary containing result names as keys and result arrays as values.

    """
    # Trim result arrays if requested.
    # This means we drop all coordinates for which all values are nan.
    if self._trim_results:
      def trim(obj):
        try:
          obj = obj.sq
        except AttributeError:
          pass
        return obj.trim()
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
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`
        The processed building block.

    """
    out = self.get_handler(block, key)(block)
    logger.debug(f"Handled {block[key]}:\n{out}")
    return out

  def get_handler(self, block, key = "type"):
    """Get the handler function for a specific building block.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block.
      key : :obj:`str`
        The key that identifies the handler to be called.

    Returns
    --------
      :obj:`callable`
        The handler function corresponding to the type of building block.

    Raises
    ------
      :obj:`exceptions.InvalidBuildingBlockError`
        If a handler for the provided building block cannot be found.

    """
    try:
      btype = block[key]
    except TypeError:
      raise exceptions.InvalidBuildingBlockError(
        "Block is not subscriptable"
      )
    except KeyError:
      raise exceptions.InvalidBuildingBlockError(
        f"Block has no {key} key"
      )
    try:
      out = getattr(self, "handle_" + btype)
    except AttributeError:
      raise exceptions.InvalidBuildingBlockError(
        f"Unknown block type: '{btype}'"
      )
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
    """Handler for data resource references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "resource".

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    out = self._factbase.retrieve(
      *block["reference"],
      extent = self._extent
    )
    return out

  def handle_result(self, block):
    """Handler for result references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "result".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    Raises
    ------
      :obj:`exceptions.UnknownResultError`
        If the referenced result is not present in the query recipe.

    """
    name = block["name"]
    if name not in self._response:
      try:
        instructions = self._recipe[name]
      except KeyError:
        raise exceptions.UnknownResultError(
          f"Recipe does not contain result '{name}'"
        )
      result = self.call_handler(instructions)
      result.name = name
      self._response[name] = result
      return result
    return self._response[name]

  def handle_self(self, block):
    """Handler for self references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "self".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    return self._get_eval_obj()

  def handle_collection(self, block):
    """Handler for cube collection references.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "collection".

    Returns
    -------
      :obj:`processor.structures.CubeCollection`

    """
    out = [self.call_handler(x) for x in block["elements"]]
    return structures.CubeCollection(out)

  def handle_processing_chain(self, block):
    """Handler for processing chains.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "processing_chain".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

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
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

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
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

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
      if isinstance(y, (list, tuple)):
        params["y"] = self.update_list_elements(y)
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
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    # Set function parameters.
    params = copy.deepcopy(block["params"])
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("extract", params)

  def handle_filter(self, block):
    """Handler for the filter verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "filter".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Evaluate filterer reference into an array.
    params["filterer"] = self.call_handler(params["filterer"])
    # Set other function parameters.
    params["trim"] = self._trim_filter
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("filter", params)

  def handle_groupby(self, block):
    """Handler for the groupby verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "groupby".

    Returns
    -------
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Evaluate grouper reference into an array or collection of arrays.
    params["grouper"] = self.call_handler(params["grouper"])
    # Call verb.
    return self.call_verb("groupby", params)

  def handle_label(self, block):
    """Handler for the label verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "label".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    return self.call_verb("label", block["params"])

  def handle_reduce(self, block):
    """Handler for the reduce verb.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "verb" and name
        "reduce".

    Returns
    -------
      :obj:`xarray.DataArray` or
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    # Get function parameters.
    params = copy.deepcopy(block["params"])
    # Obtain reducer function.
    params["reducer"] = self.get_reducer(params["reducer"])
    # Set other function parameters.
    params["track_types"] = self._track_types
    # Call verb.
    return self.call_verb("reduce", params)

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

  def handle_value_label(self, block):
    """Handler for value labels.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "value_label".

    Returns
    -------
      :obj:`int` or :obj:`float`
        The value belonging to the label.

    Raises
    -------
      :obj:`exceptions.UnknownLabelError`
        If the value label is not used for any value in the active evaluation
        object, or the active evaluation object has no value labels defined at
        all.

    """
    obj = self._get_eval_obj()
    mapping = obj.sq.value_labels
    if mapping is None:
      raise exceptions.UnknownLabelError(
        "Active evaluation object has no value labels defined"
      )
    label = block["label"]
    value = None
    for i, x in enumerate(mapping.values()):
      if x == label:
        value = list(mapping.keys())[i]
        break
    if value is None:
      raise exceptions.UnknownLabelError(
        f"There is no value with label '{label}'"
      )
    return value

  def handle_geometries(self, block):
    """Handler for spatial features.

    Parameters
    ----------
      block : :obj:`dict`
        Textual representation of a building block of type "geometries".

    Returns
    -------
      :obj:`geopandas.GeoDataFrame`

    """
    feats = block["value"]["features"]
    crs = pyproj.CRS.from_string(block["value"]["crs"])
    out = gpd.GeoDataFrame.from_features(feats, crs = crs)
    if crs != self.crs:
      out = out.to_crs(self.crs)
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
    dt = np.datetime64(block["value"]["datetime"])
    tz = pytz.timezone(block["value"]["tz"])
    if tz != self.tz.zone:
      dt = utils.convert_datetime64(dt, tz, self.tz)
    out = xr.DataArray([dt])
    out.sq.value_type = "datetime"
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
    start = np.datetime64(block["value"]["start"])
    end = np.datetime64(block["value"]["end"])
    tz = pytz.timezone(block["value"]["tz"])
    if tz != self.tz:
      start = utils.convert_datetime64(start, tz, self.tz)
      end = utils.convert_datetime64(start, tz, self.tz)
    out = xr.DataArray([start, end])
    out.sq.value_type = "datetime"
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
      :obj:`CubeCollection <semantique.processor.structures.CubeCollection>`

    """
    # Get the object to apply the verb to.
    obj = self._get_eval_obj()
    try:
      obj = obj.sq
    except AttributeError:
      pass
    # Apply the verb.
    verb = getattr(obj, name)
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
    return out

  def update_list_elements(self, obj):
    """Update the elements of a list.

    If an element in a list is a valid building block with a corresponding
    handler function, this handler function is called to proccess the building
    block.

    Parameters
    ----------
      obj : :obj:`list`
        List of which the elements should be updated.

    Returns
    -------
      :obj:`list`
        The same list with updated elements.

    """
    def _update(x):
      try:
        return self.call_handler(x)
      except exceptions.InvalidBuildingBlockError:
        return x
    return [_update(x) for x in obj]

  def add_operator(self, name, function):
    """Add a new operator to the set of supported operators.

    Parameters
    ----------
      name : :obj:`str`
        Name of the operator to be added.
      function : :obj:`callable`
        Operator function to be added.

    """
    new = {name: function}
    self._operators.update(new)

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
    try:
      obj = self._operators[name]
    except KeyError:
      raise exceptions.UnknownOperatorError(
        f"Operator '{name}' is not defined"
      )
    return obj

  def store_default_operators(self):
    """Store the built-in operators as supported operators."""
    src = semantique.processor.operators
    functions = dict(inspect.getmembers(src, inspect.isfunction))
    functions = {k:v for k, v in functions.items() if k[0] != "_"}
    out = {}
    for f in functions:
      out[f[:-1]] = functions[f]
    self._operators = out

  def add_reducer(self, name, function):
    """Add a new reducer to the set of supported reducers.

    Parameters
    ----------
      name : :obj:`str`
        Name of the reducer to be added.
      function : :obj:`callable`
        Reducer function to be added.

    """
    new = {name: function}
    self._reducers.update(new)

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
    try:
      obj = self._reducers[name]
    except KeyError:
      raise exceptions.UnknownReducerError(
        f"Reducer '{name}' is not defined"
      )
    return obj

  def store_default_reducers(self):
    """Store the built-in reducers as supported reducers."""
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