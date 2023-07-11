import numpy as np

from geopandas import GeoDataFrame
from semantique import exceptions

DTYPE_MAPPING = {
  "b": ["binary"],
  "i": ["continuous", "discrete", "ordinal", "nominal"],
  "u": ["continuous", "discrete", "ordinal", "nominal"],
  "f": ["continuous"],
  "M": ["datetime"],
  "O": ["ordinal", "nominal"],
  "U": ["ordinal", "nominal"]
}
""":obj:`dict` : Mapping between numpy data types and semantique value types.

A semantique value type describes what kind of data an array contains. It differs
from the very technical, computer-oriented `numpy dtype`_ categorization, which
contains e.g. :obj:`int`, :obj:`float`, etc. Instead, the semantique value type
describes data on a more general, statistical level. Each numpy data type can
be mapped to one or semantique value types.

In this mapping, numpy data types are expressed by their `.kind` property. Each
numpy data type may be mapped to more than one semantique value type, meaning
that it can represent any of these value types.

.. _numpy dtype:
  https://numpy.org/doc/stable/reference/arrays.dtypes.html
"""

TYPE_PROMOTION_MANUALS = {
  "not": {
    "binary": "binary",
    "__preserve_labels__": 1
  },
  "is_missing": {
    "binary": "binary",
    "continuous": "binary",
    "discrete": "binary",
    "nominal": "binary",
    "ordinal": "binary",
    "coords": "binary",
    "datetime": "binary",
    "__preserve_labels__": 0
  },
  "not_missing": {
    "binary": "binary",
    "continuous": "binary",
    "discrete": "binary",
    "nominal": "binary",
    "ordinal": "binary",
    "coords": "binary",
    "datetime": "binary",
    "__preserve_labels__": 0
  },
  "absolute": {
    "continuous": "continuous",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "ceiling": {
    "continuous": "discrete",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "cos": {
    "continuous": "discrete",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "cube_root": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "exponential": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "floor": {
    "continuous": "discrete",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "natural_logarithm": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "sin": {
    "continuous": "discrete",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "square_root": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "tan": {
    "continuous": "discrete",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "add": {
    "binary": {"binary": "discrete"},
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "discrete"},
    "__preserve_labels__": 0
  },
  "divide": {
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "continuous"},
    "__preserve_labels__": 0
  },
  "multiply": {
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "discrete"},
    "__preserve_labels__": 0
  },
  "power": {
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "discrete"},
    "__preserve_labels__": 0
  },
  "subtract": {
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "discrete"},
    "__preserve_labels__": 0
  },
  "normalized_difference": {
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "continuous"},
    "__preserve_labels__": 0
  },
  "and": {
    "binary": {"binary": "binary"},
    "__preserve_labels__": 1
  },
  "or": {
    "binary": {"binary": "binary"},
    "__preserve_labels__": 1
  },
  "exclusive_or": {
    "binary": {"binary": "binary"},
    "__preserve_labels__": 1
  },
  "equal": {
    "binary": {"binary": "binary"},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "nominal": {"nominal": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "in": {
    "binary": {"binary": "binary"},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "nominal": {"nominal": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "not_equal": {
    "binary": {"binary": "binary"},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "nominal": {"nominal": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "not_in": {
    "binary": {"binary": "binary"},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "nominal": {"nominal": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "greater": {
    "binary": {"binary": "binary",},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "greater_equal": {
    "binary": {"binary": "binary",},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "less": {
    "binary": {"binary": "binary",},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "less_equal": {
    "binary": {"binary": "binary",},
    "continuous": {"continuous": "binary", "discrete": "binary"},
    "discrete": {"continuous": "binary", "discrete": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "intersects": {
    "coords": {"coords": "binary", "geometry": "binary"},
    "__preserve_labels__": 0
  },
  "after": {
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "before": {
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "during": {
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "mean": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "median": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "mode": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "nominal": "nominal",
    "ordinal": "ordinal",
    "coords": "coords",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "max": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "ordinal": "ordinal",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "min": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "ordinal": "ordinal",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "range": {
    "continuous": "continuous",
    "discrete": "discrete",
    "datetime": "continuous",
    "__preserve_labels__": 0
  },
  "n": {
    "binary": "discrete",
    "continuous": "discrete",
    "discrete": "discrete",
    "nominal": "discrete",
    "ordinal": "discrete",
    "coords": "discrete",
    "datetime": "discrete",
    "__preserve_labels__": 0
  },
  "product": {
    "continuous": "continuous",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "standard_deviation": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "sum": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "__preserve_labels__": 0
  },
  "variance": {
    "continuous": "continuous",
    "discrete": "continuous",
    "__preserve_labels__": 0
  },
  "all": {
    "binary": "binary",
    "__preserve_labels__": 1
  },
  "any": {
    "binary": "binary",
    "__preserve_labels__": 1
  },
  "none": {
    "binary": "binary",
    "__preserve_labels__": 1
  },
  "count": {
    "binary": "discrete",
    "__preserve_labels__": 0
  },
  "percentage": {
    "binary": "continuous",
    "__preserve_labels__": 0
  },
  "first": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "nominal": "nominal",
    "ordinal": "ordinal",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "last": {
    "binary": "binary",
    "continuous": "continuous",
    "discrete": "discrete",
    "nominal": "nominal",
    "ordinal": "ordinal",
    "coords": "coords",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "assign": {
    "binary": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "continuous": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "discrete": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "nominal": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "ordinal": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "datetime": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "coords": {
      "binary": "binary",
      "continuous": "continuous",
      "discrete": "discrete",
      "nominal": "nominal",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "__preserve_labels__": 2
  },
  "assign_at": {
    "binary": {"binary": "binary"},
    "continuous": {"continuous": "continuous", "discrete": "continuous"},
    "discrete": {"continuous": "continuous", "discrete": "discrete"},
    "nominal": {"nominal": "nominal"},
    "ordinal": {"ordinal": "ordinal"},
    "datetime": {"datetime": "datetime"},
    "coords": {"coords": "coords"},
    "__preserve_labels__": 0
  }
}
""":obj:`dict` : Type promotion manuals for all built-in operators and reducers.

Whenever applying actions to an array, its value type might change. For
example, when evaluating an expression (e.g. when evaluating an expression
involving a comparison operator the resulting values are always binary) or
applying a reducer (e.g. when counting the number of "true" values in a binary
array the resulting values are discrete). This is called type promotion.
For each built-in operator and reducer function, this dictionary stores a
manual that defines which value types are accepted as input, and what the value
type of the output should be.

For reducers and univariate operators, a type promotion manual is always a
dictionary with as keys the supported input value types and as values the
output value type. For bivariate operators, the type promotion manuals have
an extra layer. The first layer of keys refers to the value type of the first
operand and the second layer of keys to the value type of the second operand.
"""

def get_value_type(x):
  """Determine the value type of an object.

  A semantique value type describes what kind of data an array contains. It
  differs from the very technical, computer-oriented `numpy dtype`_
  categorization, which contains e.g. :obj:`int`, :obj:`float`, etc. Instead,
  the semantique value type describes data on a more general, statistical
  level. Currently it makes a distinction between five main value types:
  ``continuous`` and ``discrete`` for quantitative data and ``nominal``,
  ``ordinal`` and ``binary`` for qualitative data. Additional value
  types exist for spatio-temporal data: ``datetime`` for timestamps,
  ``coords`` for spatial coordinate tuples, and ``geometry`` for spatial
  geometries stored in :obj:`geopandas.GeoDataFrame` objects.

  Parameters
  ----------
    x:
      Object to determine the value type of.

  Returns
  -------
    :obj:`list`
      A list containing the determined value type. If the object may be one of
      multiple value types this list will have multiple elements. If no value
      type could be determined for the given object, the function will return
      :obj:`None`.

  .. _numpy dtype:
    https://numpy.org/doc/stable/reference/arrays.dtypes.html

  """
  try:
    vtype = x.sq.value_type
  except AttributeError:
    try:
      vtype = x.obj.sq.value_type
    except AttributeError:
      if isinstance(x, GeoDataFrame):
        vtype = "geometry"
      else:
        x = np.array(x)
        vtype = None
  if vtype is None:
    dtype = x.dtype.kind
    try:
      vtype = DTYPE_MAPPING[dtype]
    except KeyError:
      pass
  else:
    if not isinstance(vtype, (list, tuple)):
      vtype = [vtype]
  return vtype

def get_value_labels(x):
  """Obtain the value labels of an object.

  Value labels are character strings that describe the meaning of the values in
  an array. They are mainly used for categorical data, in which integer
  indices are the value in the array, and the labels store the name of each
  category.

  Parameters
  ----------
    x:
      Object to obtain the value labels from.

  Returns
  --------
    :obj:`dict`
      The obtained value labels as a dictionary containing value-label pairs.
      If the object does not have value labels stored, the function will return
      :obj:`None`.

  """
  try:
    vlabs = x.sq.value_labels
  except AttributeError:
    try:
      vlabs = x.obj.sq.value_labels
    except AttributeError:
      vlabs = None
  return vlabs

class TypePromoter:
  """Worker that takes care of promoting value types during an operation.

  Whenever applying an operation to an array, its value type might change.
  For example, when evaluating an expression (e.g. when evaluating an expression
  involving a comparison operator the resulting values are always binary) or
  applying a reducer (e.g. when counting the number of "true" values in a binary
  array the resulting values are discrete). This is called type promotion.

  This worker takes care of the type promotion during a specified operation. It
  can check if the value types of the operands are supported by the operation,
  and if yes, determine the value type of the output of the operation.

  Parameters
  ----------
    *operands:
      The operands of the operation. Reducer functions and univariate operators
      always have a single operand, while bivariate operators have two operands.
    function : :obj:`str`, optional
      Name of the operation.
    manual : :obj:`dict`, optional
      Type promotion manual of the operation. If :obj:`None`, the worker will try
      to obtain the manual from the built-in :data:`TYPE_PROMOTION_MANUALS`
      dictionary, using the operation name (see the ``function`` parameter) as
      search key.

  """

  def __init__(self, *operands, function = None, manual = None):
    self._operands = operands
    self._function = function
    self._manual = manual
    self._input_types = None
    self._output_type = None
    self._input_labels = None
    self._output_labels = None

  @property
  def manual(self):
    """:obj:`dict`: The type promotion manual of the operation."""
    if self._manual is None:
      try:
        self._manual = TYPE_PROMOTION_MANUALS[self._function]
      except KeyError:
        pass
    return self._manual

  @property
  def input_types(self):
    """:obj:`list`: The value types of the operands."""
    if self._input_types is None:
      self._input_types = [get_value_type(x) for x in self._operands]
    return self._input_types

  @property
  def output_type(self):
    """:obj:`str`: The value type of the output."""
    if self._output_type is None:
      self.check()
    return self._output_type

  @property
  def input_labels(self):
    """:obj:`list` of `:obj:`dict`: The value labels of the operands."""
    if self._input_labels is None:
      self._input_labels = [get_value_labels(x) for x in self._operands]
    return self._input_labels

  @property
  def output_labels(self):
    """:obj:`dict`: The value labels of the output."""
    if self._output_labels is None:
      try:
        preserve_labels = self._manual["__preserve_labels__"]
      except KeyError:
        preserve_labels = 0
      if preserve_labels:
        self._output_labels = self.input_labels[preserve_labels - 1]
    return self._output_labels
  
  def check(self):
    """Check if the operation supports the operand value type(s).

    Specific operations may only be applicable to specific value types. For
    example, :func:`semantique.processor.reducers.any_` is only supported for
    arrays containing binary values, and
    :func:`semantique.processor.operators.sum_` is only supported for operands
    that are both quantitative (i.e. continuous or discrete).

    This method obtains the value type of the operand/operands and uses the type
    promotion manual to determine if the operation supports this value type/the
    combination of these value types.

    Raises
    -------
      :obj:`exceptions.InvalidValueTypeError`
        If the operation does not support the operand value type(s).
      :obj:`ValueError`
        If the type promotion manual is missing.

    """
    intypes = self.input_types
    outtype = None
    manual = self.manual
    if manual is None:
      raise ValueError(
        f"No type promotion manual defined for function '{self._function}'"
      )
    if len(intypes) == 1:
      for x in intypes[0]:
        try:
          outtype = manual[x]
          break
        except KeyError:
          continue
    else:
      if len(intypes[0]) == 1 and intypes[0][0] in intypes[1]:
        try:
          xtype = intypes[0][0]
          outtype = manual[xtype][xtype]
        except KeyError:
          pass
      if outtype is None:
        combs = [(x, y) for x in intypes[0] for y in intypes[1]]
        for ref in combs:
          try:
            outtype = manual[ref[0]][ref[1]]
            break
          except KeyError:
            continue
    if outtype is None:
      raise exceptions.InvalidValueTypeError(
        f"Unsupported operand value type(s) for '{self._function}': {intypes}"
      )
    self._output_type = outtype

  def promote(self, obj):
    """Promote the value type of the operation output.

    Parameters
    ----------
      obj : :obj:`xarray.DataArray`
        The output of the operation.

    Returns
    --------
      obj : :obj:`xarray.DataArray`
        The same object with an updated value type property.

    """
    # Set type.
    obj.sq.value_type = self.output_type
    # Set labels.
    labs = self.output_labels
    if labs is None:
      del obj.sq.value_labels
    else:
      obj.sq.value_labels = labs
    return obj