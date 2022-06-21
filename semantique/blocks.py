import builtins

from semantique import exceptions
from semantique.extent import SpatialExtent, TemporalExtent

def _parse_filter_expression(*args):
  n_args = len(args)
  if n_args == 2:
    component = None
    operator = args[0]
    y = args[1]
  elif n_args == 3:
    component = args[0]
    operator = args[1]
    y = args[2]
  else:
    raise ValueError(
      f"Filter expression should be of length 3 (component, operator, y) "
      f"or 2 (operator, y), not {n_args}"
    )
  return component, operator, y

def _parse_second_operand(x):
  if isinstance(x, (list, tuple, builtins.set)):
    out = set(x) # Note this is the def of set in this module, not built-in.
  return out

class ArrayProxy(dict):
  """Proxy object of an array.

  This proxy object serves as a placeholder for a multi-dimensional array. All
  array specific actions (i.e. *verbs*) can be called as methods of this object.
  However, the proxy object iself does not contain any data, and the actions
  are not applied. Instead, a textual recipe is constructed which will be
  executed only at the stage of query processing.

  Parameters
  ----------
    obj : :obj:`dict`
      Textual reference that can be solved by the query processor. Normally not
      constructed by hand, but obtained by calling one of the dedicated
      reference functions or by applying verbs to existing :obj:`ArrayProxy` or
      :obj:`CollectionProxy` instances.

  """

  def __init__(self, obj):
    super(ArrayProxy, self).__init__(obj)

  def _append_verb(self, name, collector = False, **kwargs):
    verb = {"type": "verb", "name": name, "params": kwargs}
    if "do" in self:
      self["do"].append(verb)
      return CollectionProxy(self) if collector else self
    else:
      new = {"type": "processing_chain", "with": self, "do": [verb]}
      return CollectionProxy(new) if collector else ArrayProxy(new)

  def evaluate(self, operator, y = None, **kwargs):
    """Evaluate an expression for each pixel in an array.

    Evaluates an expression for each pixel in the input. This expression may be
    univariate, i.e. applying an operator to a single value, or bivariate, e.g.
    an arithmetic operation (add, multiply, ..) or a condition (equals,
    greater, ..). In the bivariate case, the second operand may be a constant
    (e.g. multiply by 2) or a second array (e.g. add the pixels of two arrays).

    Parameters
    -----------
      operator : :obj:`str`
        Name of the operator to be used in the expression. Should either be one
        of the built-in operators of semantique, or a user-defined operator
        which will be provided to the query processor when executing the query
        recipe.
      y : optional
        Right-hand side of a bivariate expression. May be a constant, meaning
        that the same value is used in each expression. May also be a proxy of
        another array which can be aligned to the same shape as the input.
        Ignored when the operator is univariate.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.evaluate> <processor.structures.SemanticArray.evaluate>>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").evaluate("not")
    >>> sq.entity("water").evaluate("or", sq.entity("vegetation"))

    """
    if y is None:
      kwargs.update({"operator": operator})
    else:
      y = _parse_second_operand(y)
      kwargs.update({"operator": operator, "y": y})
    return self._append_verb("evaluate", **kwargs)

  def extract(self, dimension, component = None, **kwargs):
    """Extract coordinate labels of a dimension as a new array.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to be extracted.
      component : :obj:`str`, optional
        Name of a specific component of the dimension coordinates to be
        extracted, e.g. *year*, *month* or *day* for temporal dimension
        coordinates.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.extract <processor.structures.SemanticArray.extract>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").extract("time")
    >>> sq.entity("water").extract("time", "year")

    """
    if component is None:
      kwargs.update({"dimension": dimension})
    else:
      kwargs.update({"dimension": dimension, "component": component})
    return self._append_verb("extract", **kwargs)

  def filter(self, filterer, **kwargs):
    """Filter the values in an array.

    Filters the pixel values in the input based on a condition. The evaluated
    condition should be provided as a binary array that can be aligned to the
    same shape as the input. Only the values of pixels corresponding to a
    "true" value in the evaluated condition are preserved, the others are
    assigned a nodata value.

    Parameters
    -----------
      filterer : :obj:`ArrayProxy`
        Proxy of a binary array which can be aligned to the same shape as the
        input.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.filter <processor.structures.SemanticArray.filter>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").filter(sq.entity("cloud").evaluate("not"))
    >>> sq.entity("water").filter(sq.self())

    """
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def filter_time(self, *filterer, **kwargs):
    """Filter an array along the temporal dimension.

    This verb is a shortcut for first extracting the temporal dimension with
    the :meth:`extract` verb (optionally only a specific component of that
    dimension, such as *year* or *month*), then evaluating a relational
    operator on the coordinates of this dimension using the :meth:`evaluate`
    verb, and finally using the output of that operation as filterer in the
    :meth:`filter` verb.

    Parameters
    -----------
      *filterer :
        Either two positional arguments consisting of respectively the name of
        the relational operator and value of the right-hand side operand to be
        used in the expression, or three positional arguments consisting of
        respectively the name of the component to be extracted from the
        temporal dimension, the relational operator and value of the right-hand
        side operand to be used in the expression.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.filter <processor.structures.SemanticArray.filter>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").filter_time("after", sq.time_instant("2021-12-31"))
    >>> sq.entity("water").filter_time("year", "equals", 2020)

    """
    component, operator, y = _parse_filter_expression(*filterer)
    eval_obj = ArrayProxy({"type": "self"})
    filterer = eval_obj.extract("time", component).evaluate(operator, y)
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def filter_space(self, *filterer, **kwargs):
    """Filter an array along the spatial dimension.

    This verb is a shortcut for first extracting the spatial dimension with
    the :meth:`extract` verb (optionally only a specific component of that
    dimension, such as the spatial feature indices), then evaluating a
    relational operator on the coordinates of this dimension using the
    :meth:`evaluate` verb, and finally using the output of that operation as
    filterer in the :meth:`filter` verb.

    Parameters
    -----------
      *filterer :
        Either two positional arguments consisting of respectively the name of
        the relational operator and value of the right-hand side operand to be
        used in the expression, or three positional arguments consisting of
        respectively the name of the component to be extracted from the
        spatial dimension, the relational operator and value of the right-hand
        side operand to be used in the expression.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.filter <processor.structures.SemanticArray.filter>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").filter_space("intersects", sq.geometry(gpd.read_file("foo.gpkg")))
    >>> sq.entity("water").filter_space("feature", "equals", 1)

    """
    component, operator, y = _parse_filter_expression(*filterer)
    eval_obj = ArrayProxy({"type": "self"})
    filterer = eval_obj.extract("space", component).evaluate(operator, y)
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def groupby(self, grouper, **kwargs):
    """Group the values in an array.

    Splits the array into distinct subsets that each comprise a group of pixels
    sharing similar coordinates of a particular dimension, e.g. each group may
    correspond to a year.

    Parameters
    -----------
      grouper : :obj:`ArrayProxy` or :obj:`CollectionProxy`
        Proxy of an array containing a single dimension that is also
        present in the input. Each unique value in this array corresponds
        to a group. Alternatively it may be a proxy of a collection of such
        one-dimensional arrays. Then, each unique combination of values
        corresponds to a group.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.groupby <processor.structures.SemanticArray.groupby>`.

    Returns
    --------
      :obj:`CollectionProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").groupby(sq.entity("water").extract("time", "year"))

    """
    kwargs.update({"grouper": grouper})
    return self._append_verb("groupby", collector = True, **kwargs)

  def groupby_time(self, component = None, **kwargs):
    """Group an array along the temporal dimension.

    This verb is a shortcut for first extracting the temporal dimension with
    the :meth:`extract` verb (usually only a specific component of that
    dimension, such as *year* or *month*), and then using the resulting
    one-dimensional array as grouper in the :meth:`groupby` verb.

    Parameters
    -----------
      component : :obj:`str`, optional
        Name of a specific component of the temporal dimension coordinates to
        be extracted, e.g. *year*, *month* or *day*.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.groupby <processor.structures.SemanticArray.groupby>`.

    Returns
    --------
      :obj:`CollectionProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").groupby_time("year")

    """
    eval_obj = ArrayProxy({"type": "self"})
    if isinstance(component, list):
      comps = [eval_obj.extract("time", x) for x in component]
      grouper = CollectionProxy({"type": "collection", "elements": comps})
    else:
      grouper = eval_obj.extract("time", component)
    kwargs.update({"grouper": grouper})
    return self._append_verb("groupby", collector = True, **kwargs)

  def groupby_space(self, component = None, **kwargs):
    """Group an array along the spatial dimension.

    This verb is a shortcut for first extracting the spatial dimension with
    the :meth:`extract` verb (usually only a specific component of that
    dimension, such as the spatial feature indices), and then using the
    resulting one-dimensional array as grouper in the :meth:`groupby` verb.

    Parameters
    -----------
      component : :obj:`str`, optional
        Name of a specific component of the spatial dimension coordinates to
        be extracted, e.g. *feature*.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.groupby <processor.structures.SemanticArray.groupby>`.

    Returns
    --------
      :obj:`CollectionProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").groupby_space("feature")

    """
    eval_obj = ArrayProxy({"type": "self"})
    if isinstance(component, list):
      comps = [eval_obj.extract("space", x) for x in component]
      grouper = CollectionProxy({"type": "collection", "elements": comps})
    else:
      grouper = eval_obj.extract("space", component)
    kwargs.update({"grouper": grouper})
    return self._append_verb("groupby", collector = True, **kwargs)

  def reduce(self, dimension, reducer, **kwargs):
    """Reduce the dimensionality of an array.

    Reduces dimensionality by applying a reducer function that returns a single
    value for each column along a given dimension, e.g. a summary statistic or
    a boolean.

    See `here`_ for an overview of the built-in reducer functions that can be
    chosen from (they should be referred to by their name, without the
    underscore at the end).

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to be reduced.
      operator : :obj:`str`
        Name of the reducer function to be applied.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.reduce <processor.structures.SemanticArray.reduce>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").reduce("time", "count")

    .. _here:
      https://zgis.github.io/semantique/reference.html#reducer-functions

    """
    kwargs.update({"dimension": dimension, "reducer": reducer})
    return self._append_verb("reduce", **kwargs)

  def shift(self, dimension, steps, **kwargs):
    """Shift the values in an array.

    Shifts the pixel values a given number of steps along a given dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to shift along.
      steps : :obj:`int`
        Amount of steps each value should be shifted. A negative integer will
        result in a shift to the left, while a positive integer will result in
        a shift to the right.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.shift <processor.structures.SemanticArray.shift>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").shift("time", 1)

    """
    kwargs.update({"dimension": dimension, "steps": steps})
    return self._append_verb("shift", **kwargs)

  def smooth(self, dimension, reducer, size, **kwargs):
    """Smooth the values in an array.

    Smoothes the pixel values by applying a moving window function along a
    given dimension.

    The moving window functions are reducer functions that reduce the values in
    the window to a single value. See `here`_ for an overview of the built-in
    reducer functions that can be chosen from (they should be referred to by
    their name, without the underscore at the end).

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to smooth along.
      reducer : :obj:`str`
        Name of the reducer function to be used to reduce the values in the
        moving window.
      size : :obj:`int`
        Size k defining the extent of the rolling window. The pixel being
        smoothed will always be in the center of the window, with k pixels at
        its left and k pixels at its right. If the dimension to smooth over is
        the spatial dimension, the size will be used for both the X and Y
        dimension, forming a square window with the smoothed pixel in the
        middle.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.smooth <processor.structures.SemanticArray.smooth>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").smooth("time", "any", 1)

    .. _here:
      https://zgis.github.io/semantique/reference.html#reducer-functions

    """
    kwargs.update({"dimension": dimension, "reducer": reducer, "size": size})
    return self._append_verb("smooth", **kwargs)


  def name(self, name, **kwargs):
    """Give a name to an array.

    Parameters
    -----------
      name : :obj:`str`
        Name to be given to the input.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`SemanticArray.name <processor.structures.SemanticArray.name>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.entity("water").name("foo")

    """
    kwargs.update({"name": name})
    return self._append_verb("name", **kwargs)

class CollectionProxy(dict):
  """Proxy object of an array collection.

  This proxy object serves as a placeholder for a collection of multiple
  multi-dimensional arrays. All collection specific actions (i.e. *verbs*) can
  be called as methods of this object. However, the proxy object iself does not
  contain any data, and the actions are not applied. Instead, a textual recipe
  is constructed which will be executed only at the stage of query processing.

  Parameters
  ----------
    obj : :obj:`dict`
      Textual reference that can be solved by the query processor. Normally not
      constructed by hand, but obtained by calling one of the dedicated
      reference functions or by applying verbs to existing :obj:`ArrayProxy` or
      :obj:`CollectionProxy` instances.

  """

  def __init__(self, obj):
    super(CollectionProxy, self).__init__(obj)

  def _append_verb(self, name, combiner = False, **kwargs):
    verb = {"type": "verb", "name": name, "params": kwargs}
    if "do" in self:
      self["do"].append(verb)
      return ArrayProxy(self) if combiner else self
    else:
      new = {"type": "processing_chain", "with": self, "do": [verb]}
      return ArrayProxy(new) if combiner else CollectionProxy(new)

  def compose(self, **kwargs):
    """Create a categorical composition of multiple binary arrays.

    The categorical composition contains a value of 1 for each pixel being
    "true" in the first array of the collection, a value of 2 for each pixel
    being "true" in the second array of the collection, et cetera. If a pixel
    is "true" in more than one array of the collection, the first of those is
    prioritized. If a pixel is not "true" in any array of the collection it
    gets assigned a nodata value.

    Parameters
    -----------
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`Collection.compose <processor.structures.Collection.compose>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.collection(sq.entity("water"), sq.entity("ice")).compose()

    """
    return self._append_verb("compose", combiner = True)

  def concatenate(self, dimension, **kwargs):
    """Concatenate multiple arrays along a new or existing dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to concatenate along. To concatenate along an
        existing dimension, it should be a dimension that exists in all
        collection members. To concatenate along a new dimension, it should be
        a dimension that does not exist in any of the collection members.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`Collection.concatenate <processor.structures.Collection.concatenate>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.collection(sq.entity("water"), sq.entity("ice")).concatenate("concept")

    """
    kwargs.update({"dimension": dimension})
    return self._append_verb("concatenate", combiner = True, **kwargs)

  def merge(self, reducer, **kwargs):
    """Merge pixel values of multiple arrays into a single value per pixel.

    The values of corresponding pixels (i.e. having the same dimension
    coordinates) are merged by applying a reducer function. See `here`_ for an
    overview of the built-in reducer functions that can be chosen from (they
    should be referred to by their name, without the underscore at the end).

    Parameters
    -----------
      reducer : :obj:`str`
        Name of the reducer function to be applied.
      **kwargs:
        Additional keyword arguments passed on to
        :obj:`Collection.merge <processor.structures.Collection.merge>`.

    Returns
    --------
      :obj:`ArrayProxy`

    Examples
    --------
    >>> import semantique as sq
    >>> sq.collection(sq.entity("water"), sq.entity("ice")).merge("or")

    """
    kwargs.update({"reducer": reducer})
    return self._append_verb("merge", combiner = True, **kwargs)

  def evaluate(self, operator, y = None, **kwargs):
    """Apply the evaluate verb to each array in a collection.

    See :meth:`ArrayProxy.evaluate`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    if y is None:
      kwargs.update({"operator": operator})
    else:
      kwargs.update({"operator": operator, "y": y})
    return self._append_verb("evaluate", **kwargs)

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to each array in a collection.

    See :meth:`ArrayProxy.extract`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    if component is None:
      kwargs.update({"dimension": dimension})
    else:
      kwargs.update({"dimension": dimension, "component": component})
    return self._append_verb("extract", **kwargs)

  def filter(self, filterer, **kwargs):
    """Apply the filter verb to each array in a collection.

    See :meth:`ArrayProxy.filter`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def filter_time(self, *filterer, **kwargs):
    """Apply the filter_time verb to each array in a collection.

    See :meth:`ArrayProxy.filter_time`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    component, operator, y = _parse_filter_expression(*filterer)
    eval_obj = ArrayProxy({"type": "self"})
    filterer = eval_obj.extract("time", component).evaluate(operator, y)
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def filter_space(self, *filterer, **kwargs):
    """Apply the filter_space verb to each array in a collection.

    See :meth:`ArrayProxy.filter_space`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    component, operator, y = _parse_filter_expression(*filterer)
    eval_obj = ArrayProxy({"type": "self"})
    filterer = eval_obj.extract("space", component).evaluate(operator, y)
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def reduce(self, dimension, reducer, **kwargs):
    """Apply the reduce verb to each array in a collection.

    See :meth:`ArrayProxy.reduce`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    kwargs.update({"dimension": dimension, "reducer": reducer})
    return self._append_verb("reduce", **kwargs)

  def shift(self, dimension, steps, **kwargs):
    """Apply the shift verb to each array in a collection.

    See :meth:`ArrayProxy.shift`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    kwargs.update({"dimension": dimension, "steps": steps})
    return self._append_verb("shift", **kwargs)

  def smooth(self, dimension, reducer, size, **kwargs):
    """Apply the smooth verb to each array in a collection.

    See :meth:`ArrayProxy.smooth`.

    Returns
    --------
      :obj:`CollectionProxy`

    """
    kwargs.update({"dimension": dimension, "reducer": reducer, "size": size})
    return self._append_verb("smooth", **kwargs)

def concept(*reference, property = None):
  """Reference to a semantic concept.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the semantic concept in the mapping against which the query
      is processed.
    property : :obj:`str`
      Name of a property of the referenced semantic concept. If given, only
      this property of the semantic concept is translated. If :obj:`None`,
      all properties of the semantic concept are translated and combined into a
      single semantic array.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the concept that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.concept("entity", "water")

  """
  obj = {"type": "concept", "reference": reference}
  return ArrayProxy(obj)

def entity(*reference, property = None):
  """Reference to a semantic concept being an entity.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the semantic concept within the "entity" category of the
      mapping against which the query is processed.
    property : :obj:`str`
      Name of a property of the referenced semantic concept. If given, only
      this property of the semantic concept is translated. If :obj:`None`,
      all properties of the semantic concept are translated and combined into a
      single semantic array.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the concept that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.entity("water")

  """
  obj = {"type": "concept", "reference": ("entity",) + reference}
  if property is not None:
    obj["property"] = property
  return ArrayProxy(obj)

def event(*reference, property = None):
  """Reference to a semantic concept being an event.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the semantic concept within the "event" category of the
      mapping against which the query is processed.
    property : :obj:`str`
      Name of a property of the referenced semantic concept. If given, only
      this property of the semantic concept is translated. If :obj:`None`,
      all properties of the semantic concept are translated and combined into a
      single semantic array.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the concept that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.event("flood")

  """
  obj = {"type": "concept", "reference": ("event",) + reference}
  if property is not None:
    obj["property"] = property
  return ArrayProxy(obj)

def layer(*reference):
  """Reference to a data layer in an EO data cube.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer in the layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.layer("appearance", "brightness")

  """
  obj = {"type": "layer", "reference": reference}
  return ArrayProxy(obj)

def appearance(*reference):
  """Reference to a data layer describing appearance.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer within the "appearance" category of the
      layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.appearance("brightness")

  """
  obj = {"type": "layer", "reference": ("appearance",) + reference}
  return ArrayProxy(obj)

def artifacts(*reference):
  """Reference to a data layer describing artifacts.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer within the "artifacts" category of the
      layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  """
  obj = {"type": "layer", "reference": ("artifacts",) + reference}
  return ArrayProxy(obj)

def atmosphere(*reference):
  """Reference to a data layer describing atmosphere.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer within the "atmosphere" category of the
      layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.atmosphere("Color type")

  """
  obj = {"type": "layer", "reference": ("atmosphere",) + reference}
  return ArrayProxy(obj)

def reflectance(*reference):
  """Reference to a data layer describing reflectance.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer within the "reflectance" category of the
      layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.reflectance("s2_band01")

  """
  obj = {"type": "layer", "reference": ("reflectance",) + reference}
  return ArrayProxy(obj)

def topography(*reference):
  """Reference to a data layer describing topography.

  Parameters
  ----------
    *reference : :obj:`str`
      The index of the data layer within the "topography" category of the
      layout of the EO data cube.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the layer that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.topography("elevation")

  """
  obj = {"type": "layer", "reference": ("topography",) + reference}
  return ArrayProxy(obj)

def result(name):
  """Reference to a another result definition.

  Parameters
  ----------
    name : :obj:`str`
      Name of an existing result definition in the query recipe.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the result that can be solved by the query
      processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.result("water_count")

  """
  obj = {"type": "result", "name": name}
  return ArrayProxy(obj)

def self():
  """Reference to the active evaluation object itself.

  Returns
  -------
    :obj:`ArrayProxy`
      A textual reference to the active evaluation object that can be
      solved by the query processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.self()

  """
  obj = {"type": "self"}
  return ArrayProxy(obj)

def collection(*elements):
  """Reference to a collection of multiple arrays.

  Parameters
  ----------
    *elements : :obj:`ArrayProxy`
      Elements of the collection.

  Returns
  -------
    :obj:`CollectionProxy`
      A textual reference to the collection that can be solved by the
      query processor.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.collection(sq.entity("water"), sq.entity("vegetation"))

  """
  obj = {"type": "collection", "elements": list(elements)}
  return CollectionProxy(obj)

def label(x):
  """Character label of a numeric index.

  Object representing a character label of a numeric index. Can be used to
  query values in an array by their label rather than by the value itself.

  Parameters
  ----------
    x : :obj:`str`
      The character label of the value of interest.

  Returns
  -------
    :obj:`dict`
      JSON-serializable object that contains the label, and can be
      understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.label("high")

  """
  obj = {"type": "label", "content": x}
  return obj

def set(*members):
  """Finite set of values.

  Object representing a finite set of values.

  Parameters
  ----------
    *members:
      The members of the set.

  Returns
  --------
    :obj:`dict`
      JSON-serializable object that contains the members of the set, and
      can be understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.set(21, 22, 23, 24)

  """
  obj = {"type": "set", "content": list(builtins.set(members))}
  return obj

def interval(a, b):
  """Interval of numerical or ordinal values.

  Object representing a set of values that fall inside the given bounds of an
  interval. The interval is closed at both sides, meaning that both of its
  bounds belong to the set. By definition, the given bounds should be ordinal
  or numerical values, and the lower bound should be smaller than or equal to
  the upper bound.

  Parameters
  ----------
    a : :obj:`int` or :obj:`float`
      The lower bound of the interval.
    b : :obj:`int` or :obj:`float`
      The upper bound of the interval.

  Returns
  --------
    :obj:`dict`
      JSON-serializable object that contains the bounds of the interval, and
      can be understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.interval(21, 24)

  """
  if b < a:
    raise exceptions.InvalidIntervalError(
      "The lower bound of an interval cannot be smaller than the upper bound"
    )
  obj = {"type": "interval", "content": [a, b]}
  return obj

def geometry(x, **kwargs):
  """Spatial geometry object.

  Object representing a spatial vector geometry, such as a point, a line, or a
  polygon. Can be used to apply a spatial filter to an array.

  Parameters
  ----------
    x
      A spatial feature formatted as an object that can be understood by the
      initializer of :class:`SpatialExtent <extent.SpatialExtent>`. This
      includes :obj:`geopandas.GeoDataFrame` objects. If multiple features
      are given they will be unioned into a multi-geometry, e.g. a multi-point,
      multi-linestring or multi-polygon.
    **kwargs
      Additional keyword arguments passed on to the initializer of
      :class:`SpatialExtent <extent.SpatialExtent>`.

  Returns
  -------
    :obj:`dict`
      JSON-serializable object that contains the spatial vector geometry, and
      can be understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> import geopandas as gpd
  >>> sq.geometry(gpd.read_file("files/parcels.geojson"))

  """
  obj = {"type": "geometry", "content": SpatialExtent(x, **kwargs)}
  return obj

def time_instant(x, **kwargs):
  """Time instant.

  Object representing a single time instant. Can be used to apply a temporal
  filter to an array.

  Parameters
  ----------
    x
      A time instant formatted as an object that can be understood by the
      initializer of :class:`TemporalExtent <extent.TemporalExtent>`. This
      includes :obj:`pandas.Timestamp` objects, as well as text representations
      of time instants in different formats.
    **kwargs
      Additional keyword arguments passed on to the initializer of
      :class:`TemporalExtent <extent.TemporalExtent>`.

  Returns
  -------
    :obj:`dict`
      JSON-serializable object that contains the time instant, and
      can be understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.time_instant("2021-12-31")

  """
  obj = {"type": "time_instant", "content": TemporalExtent(x, **kwargs)}
  return obj

def time_interval(a, b, **kwargs):
  """Time interval.

  Object representing a set of timestamps that fall inside the given bounds of
  a time interval. The interval is closed at both sides, meaning that both of
  its bounds belong to the set. By definition, the given bounds should be time
  instants, and the lower bound should be smaller than or equal to the upper
  bound.

  Parameters
  ----------
    a
      The lower bound of the interval, which should be a time instant formatted
      as an object that can be understood by the initializer of
      :class:`TemporalExtent <extent.TemporalExtent>`. This includes
      :obj:`pandas.Timestamp` objects, as well as text representations of time
      instants in different formats.
    b
      The upper bound of the interval, which should be a time instant formatted
      as an object that can be understood by the initializer of
      :class:`TemporalExtent <extent.TemporalExtent>`. This includes
      :obj:`pandas.Timestamp` objects, as well as text representations of time
      instants in different formats.
    **kwargs
      Additional keyword arguments passed on to the initializer of
      :class:`TemporalExtent <extent.TemporalExtent>`.

  Returns
  -------
    :obj:`dict`
      JSON-serializable object that contains the bounds of the time
      interval, and can be understood by the query processor as such.

  Examples
  --------
  >>> import semantique as sq
  >>> sq.time_interval("2021-01-01", "2021-12-31")

  """
  obj = {"type": "time_interval", "content": TemporalExtent(a, b, **kwargs)}
  return obj