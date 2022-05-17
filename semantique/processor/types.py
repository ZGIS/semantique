import numpy as np

from geopandas import GeoDataFrame
from semantique import exceptions

DTYPE_MAPPING = {
  "b": "binary",
  "i": ["numerical", "ordinal", "nominal"],
  "u": ["numerical", "ordinal", "nominal"],
  "f": "numerical",
  "M": "datetime",
  "O": ["ordinal", "nominal"],
  "U": ["ordinal", "nominal"]
}
""":obj:`dict` : Mapping between numpy data types and semantique value types.

A semantique value type describes what kind of data a cube contains. It differs
from the very technical, computer-oriented `numpy dtype`_ categorization, which
contains e.g. :obj:`int`, :obj:`float`, etc. Instead, the semantique value type
describes data on a more general, statistical level. Each numpy data type can
be mapped to one or semantique value types.

In this mapping, numpy data types are expressed by their `.kind` property. If
a numpy data type can be mapped to more than one semantique value type, these
value types are stored in a list.

.. _numpy dtype:
  https://numpy.org/doc/stable/reference/arrays.dtypes.html
"""

TYPE_PROMOTION_MANUALS = {
  "invert": {
    "binary": "binary",
    "__preserve_labels__": 1
  },
  "absolute": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "cube_root": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "exponential": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "natural_logarithm": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "square_root": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "add": {
    "numerical": {"numerical": "numerical"},
    "__preserve_labels__": 0
  },
  "divide": {
    "numerical": {"numerical": "numerical"},
    "__preserve_labels__": 0
  },
  "multiply": {
    "numerical": {"numerical": "numerical"},
    "__preserve_labels__": 0
  },
  "power": {
    "numerical": {"numerical": "numerical"},
    "__preserve_labels__": 0
  },
  "subtract": {
    "numerical": {"numerical": "numerical"},
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
    "nominal": {"nominal": "binary"},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "in": {
    "binary": {"binary": "binary"},
    "nominal": {"nominal": "binary"},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "not_equal": {
    "binary": {"binary": "binary"},
    "nominal": {"nominal": "binary"},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "not_in": {
    "binary": {"binary": "binary"},
    "nominal": {"nominal": "binary"},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "coords": {"coords": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "greater": {
    "binary": {"binary": "binary",},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "greater_equal": {
    "binary": {"binary": "binary",},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "less": {
    "binary": {"binary": "binary",},
    "numerical": {"numerical": "binary"},
    "ordinal": {"ordinal": "binary"},
    "datetime": {"datetime": "binary"},
    "__preserve_labels__": 0
  },
  "less_equal": {
    "binary": {"binary": "binary",},
    "numerical": {"numerical": "binary"},
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
  "assign": {
    "binary": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "nominal": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "numerical": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "ordinal": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "datetime": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "coords": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "coords": "coords",
      "datetime": "datetime"
    },
    "__preserve_labels__": 2
  },
  "mean": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "product": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "standard_deviation": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "sum": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "variance": {
    "numerical": "numerical",
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
  "count": {
    "binary": "numerical",
    "__preserve_labels__": 0
  },
  "percentage": {
    "binary": "numerical",
    "__preserve_labels__": 0
  },
  "max": {
    "binary": "binary",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "median": {
    "numerical": "numerical",
    "__preserve_labels__": 0
  },
  "min": {
    "binary": "binary",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "first": {
    "binary": "binary",
    "nominal": "nominal",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "coords": "coords",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "last": {
    "binary": "binary",
    "nominal": "nominal",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "coords": "coords",
    "datetime": "datetime",
    "__preserve_labels__": 1
  },
  "mode": {
    "binary": "binary",
    "nominal": "nominal",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "coords": "coords",
    "datetime": "datetime",
    "__preserve_labels__": 1
  }
}
""":obj:`dict` : Type promotion manuals for all built-in operators and reducers.

Whenever applying actions to a data cube, its value type might change. For
example, when evaluating an expression (e.g. when evaluating an expression
involving a comparison operator the resulting values are always binary) or
applying a reducer (e.g. when counting the number of "true" values in a binary
data cube the resulting values are numerical). This is called type promotion.
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

  A semantique value type describes what kind of data a cube contains. It
  differs from the very technical, computer-oriented `numpy dtype`_
  categorization, which contains e.g. :obj:`int`, :obj:`float`, etc. Instead,
  the semantique value type describes data on a more general, statistical
  level. Currently it makes a distinction between three main value types:
  ``numerical``, ``nominal``, ``ordinal`` and ``binary``. Additional value
  types exist for spatio-temporal data: ``datetime`` for timestamps,
  ``coords`` for spatial coordinate tuples, and ``geometry`` for spatial
  geometries stored in :obj:`geopandas.GeoDataFrame` objects.

  Parameters
  ----------
    x:
      Object to determine the value type of.

  Returns
  -------
    :obj:`str`
      The determined value type. If no value type could be determined for the
      given object, the function will return ``None``.

  .. _numpy dtype:
    https://numpy.org/doc/stable/reference/arrays.dtypes.html

  """
  try:
    vtype = x.sq.value_type
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
  return vtype

def get_value_labels(x):
  """Obtain the value labels of an object.

  Value labels are character strings that describe the meaning of the values in
  a data cube. They are mainly used for categorical data, in which integer
  indices are the value in the cube, and the labels store the name of each
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
      ``None``.

  """
  try:
    vlabs = x.sq.value_labels
  except AttributeError:
    vlabs = None
  return vlabs

class TypePromoter:
  """Worker that takes care of promoting value types during an operation.

  Whenever applying an operation to a data cube, its value type might change.
  For example, when evaluating an expression (e.g. when evaluating an expression
  involving a comparison operator the resulting values are always binary) or
  applying a reducer (e.g. when counting the number of "true" values in a binary
  data cube the resulting values are numerical). This is called type promotion.

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
      Type promotion manual of the operation. If ``None``, the worker will try
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
    data cubes containing binary values, and
    :func:`semantique.processor.operators.sum_` is only supported for operands
    that are both numerical.

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
    # Get value types of operands.
    intypes = self.input_types
    try:
      ytype = intypes[1]
    except IndexError:
      pass
    else:
      xtype = intypes[0]
      if isinstance(ytype, (list, tuple)):
        if xtype in ytype:
          ytype = xtype
        else:
          ytype = ytype[0]
      intypes = [xtype, ytype]
    # Infer value type of output.
    out = self.manual # Initialize before scanning.
    if out is None:
      raise ValueError(
        f"No type promotion manual defined for function '{self._function}'"
      )
    for x in intypes:
      try:
        out = out[x]
      except KeyError:
        raise exceptions.InvalidValueTypeError(
          f"Unsupported operand value type(s) for '{self._function}': '{intypes}'"
        )
    self._output_type = out

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