class AlignmentError(Exception):
  """Raised when a data cube cannot be aligned to a given shape.

  This occurs for example when evaluating multivariate expressions for
  pixels in a data cube using the evaluate verb, filtering values in a
  data cube using the filter verb, or splitting a data cube into distinct
  groups using the groupby verb. In these processes a second data cube is
  involved, which is aligned to the same shape as the input cube before
  applying the operation, such that each of its pixels has a corresponding
  pixel in the input cube. In some cases alignment is not possible, e.g. when
  the second cube has no dimensions in common with the input cube or when the
  second cube has more dimensions than the input cube.

  """
  pass

class EmptyDataError(Exception):
  """Raised when a retrieved data cube does not contain any valid values.

  This occurs for example when the spatio-temporal extent for which the data
  are retrieved does not intersect with the spatio-temporal extent of the
  factbase, or when all observations within that extent happen to be erroneous.

  """
  pass

class InvalidValueTypeError(Exception):
  """Raised when a data cube has a value type which a process does not allow.

  This occurs for example when evaluating expressions for pixels in a data cube
  using the evaluate verb, or when reducing the dimensionality of a data cube
  using the reduce verb. These processes allow to choose from a wide range of
  respectively operator an reducer function, each of which can only be sensibly
  used when the inputs are of a certain value type. It may also occur in the
  concatenate verb, which requires all provided data cubes to have the same
  value type, the compose verb, which requires all provided data cubes to be
  binary, and the filter verb, which requires the filterer cube to be binary.

  Note
  -----
    You may disable value type tracking by setting ``track_types = False`` when
    executing a query recipe.

  """
  pass

class InvalidValueRangeError(Exception):
  """Raised when the end of a value range is smaller than its start."""
  pass

class InvalidBuildingBlockError(Exception):
  """Raised when a query recipe contains an invalid building block.

  This occurs for example when the building block is not dict-like, when the
  building block is missing a "type" key, or when the value of the "type" key
  does not correspond to a specific handler function of the query processor.

  """
  pass

class UnknownConceptError(Exception):
  """Raised when an unexisting semantic concept is referenced.

  This occurs when a referenced semantic concept is not defined in the
  ontology against which the query is processed, or when a referenced
  property of a semantic concept is not defined for that concept.

  """
  pass

class UnknownResourceError(Exception):
  """Raised when an unexisting data resource is referenced.

  This occurs when a referenced data resource is not present in the factbase
  against which the query is processed.

  """
  pass

class UnknownResultError(Exception):
  """Raised when an unexisting query result is referenced.

  This occurs when a referenced result is not present in the same query recipe.

  """
  pass

class UnknownReducerError(Exception):
  """Raised when an undefined reducer function is used in the reduce verb.

  This occurs because the reducer function is not built-in in semantique,
  or is not added to the query processor as a custom reducer function.

  """
  pass

class UnknownOperatorError(Exception):
  """Raised when an undefined operator function is used in the evaluate verb.

  This occurs because the operator function is not built-in in semantique,
  or is not added to the query processor as a custom operator function.

  """
  pass

class UnknownDimensionError(Exception):
  """Raised when an unexisting dimension is referenced.

  This occurs for example when one wants to extract coordinates of a specific
  dimension from a data cube that does not contain this dimension, or try to
  reduce a data cube along an unexisting dimension.

  """
  pass

class UnknownComponentError(Exception):
  """Raised when an unexisting dimension component is referenced.

  This occurs when one wants to extract a specific component from a dimension
  that does not contain this component, for example when trying to extract
  the "year" component from the spatial dimension, or the "y" component from
  the temporal dimension.

  """
  pass

class UnknownLabelError(Exception):
  """Raised when a referenced value label is not used for any value in a cube.

  This occurs when one tries to query values in a data cube by their label
  instead of actual stored data value (using the obj:`value_label` block)
  but the given label is not attached to any of the values in the cube.

  """
  pass

class TooManyDimensionsError(Exception):
  """Raised when a data cube has more dimensions than a process allows.

  This occurs in functions that put a limit on the allowed number of dimensions
  a data cube may have. This includes grouping with the groupby verb, in which
  the grouper cube may only have one dimension. The documentation of such
  functions should always clearly mention these requirements.

  """
  pass

class MissingDimensionError(Exception):
  """Raised when a data cube does not contain a required dimension.

  This occurs in functions that require one or more specific dimensions to be
  present in a data cube. This includes grouping with the groupby verb, which
  requires the dimension of the grouper to be present in the input data cube,
  and retrieving data from a factbase, which requires often that at least a
  spatial dimension is present.

  """
  pass

class MixedDimensionsError(Exception):
  """Raised when the data cubes in a cube collection have differing dimensions.

  This occurs in functions that require all cubes in a cube collection to have
  exactly the same dimensions. This includes grouping with the groupby verb,
  which allows a collection of cubes to be used as grouper argument, but only
  when their dimensions are the same.

  """
  pass

class MixedTimeZonesError(Exception):
  """Raised when the bounds of a time interval have differing time zones.

  This occurs when a temporal extent is initialized by providing the start and
  end of a time interval as time instants expressed in two different time
  zones.

  """
  pass

