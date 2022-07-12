class AlignmentError(Exception):
  """Raised when an array cannot be aligned to a given shape.

  This occurs for example when evaluating bivariate expressions on pixels in an
  array using the evaluate verb, filtering values in an array using the filter
  verb, or splitting an array into distinct groups using the groupby verb. In
  all these processes a second array is involved, which is aligned to the same
  shape as the input, such that each of its pixels has a corresponding pixel in
  the input. In some cases alignment is not possible, e.g. when the second
  array has no dimensions in common with the input or when it has more
  dimensions than the input.

  """
  pass

class EmptyDataError(Exception):
  """Raised when a retrieved array does not contain any valid values.

  This occurs for example when the spatio-temporal extent for which the data
  are retrieved does not intersect with the spatio-temporal extent of the
  EO data cube, or when all observations within that extent happen to be
  erroneous.

  """
  pass

class InvalidValueTypeError(Exception):
  """Raised when an array has a value type which a process does not allow.

  This occurs for example when evaluating expressions for pixels in an array
  using the evaluate verb, or when reducing the dimensionality of an array
  using the reduce verb. These processes allow to choose from a wide range of
  respectively operator an reducer functions, each of which can only be sensibly
  used when the inputs are of a certain value type. It may also occur in the
  concatenate verb, which requires all provided arrays to have the same
  value type, the compose verb, which requires all provided arrays to be
  binary, and the filter verb, which requires the filterer to be binary.

  Note
  -----
    You may disable value type tracking by setting ``track_types = False`` when
    executing a query recipe.

  """
  pass

class InvalidIntervalError(Exception):
  """Raised when an intervals upper bound is smaller than its lower bound."""
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
  mapping against which the query is processed, or when a referenced
  property of a semantic concept is not defined for that concept.

  """
  pass

class UnknownLayerError(Exception):
  """Raised when an unexisting data layer is referenced.

  This occurs when a referenced data layer is not present in the EO data cube
  against which the query is processed.

  """
  pass

class UnknownResultError(Exception):
  """Raised when an unexisting query result is referenced.

  This occurs when a referenced result is not present in the same query recipe.

  """
  pass

class UnknownReducerError(Exception):
  """Raised when an undefined reducer function is used.

  This occurs because the reducer function is not provided by semantique
  nor added to the query processor as a custom reducer function.

  """
  pass

class UnknownOperatorError(Exception):
  """Raised when an undefined operator function.

  This occurs because the operator function is not provided by semantique
  nor added to the query processor as a custom operator function.

  """
  pass

class UnknownDimensionError(Exception):
  """Raised when an unexisting dimension is referenced.

  This occurs for example when one wants to extract coordinates of a specific
  dimension from an array that does not contain this dimension, or try to
  reduce an array along an unexisting dimension.

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
  """Raised when a referenced label is not used for any value in an array.

  This occurs when one uses the :func:`label` block to query values in an array
  by their label rather than by the value itself, but the referenced label is
  not attached to any of the values in the array.

  """
  pass

class TooManyDimensionsError(Exception):
  """Raised when an array has more dimensions than a process allows.

  This occurs in functions that put a limit on the allowed number of dimensions
  an array may have. This includes grouping with the groupby verb, in which
  the grouper may only have one dimension. The documentation of such functions
  should always clearly mention these requirements.

  """
  pass

class MissingDimensionError(Exception):
  """Raised when an array does not contain a required dimension.

  This occurs in functions that require one or more specific dimensions to be
  present in an array. This includes grouping with the groupby verb, which
  requires the dimension of the grouper to be present in the input array;
  retrieving data from the EO data cube, which often requires that at least a
  spatial dimension is present; and concatenating over an existing dimension,
  which requires this dimension to exist in all input arrays.

  """
  pass

class MixedDimensionsError(Exception):
  """Raised when arrays in a collection have differing dimensions.

  This occurs in functions that require all arrays in a collection to have
  exactly the same dimensions. This includes grouping with the groupby verb,
  which allows a collection of arrays to be used as grouper argument, but only
  when their dimensions are the same.

  """
  pass

class ReservedDimensionError(Exception):
  """Raised when a reserved dimension name is being used for a new dimension.

  This occurs in functions that can add a new dimension to an array, such as
  the concatenate verb. Semantique reserves specific dimension names for
  respectively the temporal dimension and the spatial dimensions. These names
  should not be used for any other dimension.

  """
  pass

class MixedTimeZonesError(Exception):
  """Raised when the bounds of a time interval have differing time zones.

  This occurs when a temporal extent is initialized by providing the start and
  end of a time interval as time instants expressed in two different time
  zones.

  """
  pass

