from semantique import exceptions

class Interval(list):
  """Object to represent a set of values within the bounds of an interval.

  The interval is closed at both sides, meaning that both of its bounds belong
  to the set. By definition, the given bounds should be ordinal, continuous or
  discrete values, and the lower bound should be smaller than or equal to the
  upper bound.

  Parameters
  ----------
    a : :obj:`int` or :obj:`float`
      The lower bound of the interval.
    b : :obj:`int` or :obj:`float`
      The upper bound of the interval.

  """

  def __init__(self, lower, upper):
    if upper < lower:
      raise exceptions.InvalidIntervalError(
        "The lower bound of an interval cannot be smaller than its upper bound"
      )
    super(Interval, self).__init__([lower, upper])

  @property
  def sq(self):
    """self: Semantique accessor.

    This is merely provided to ensure compatible behaviour with
    :obj:`Array <semantique.processor.arrays.Array>` objects, which are
    modelled as an accessor to :obj:`xarray.DataArray` objects. It allows to
    call all other properties and methods through the prefix ``.sq``.

    """
    return self

  @property
  def lower(self):
    """:obj:`int` or :obj:`float`: The lower bound of the interval."""
    return self[0]

  @property
  def upper(self):
    """:obj:`int` or :obj:`float`: The upper bound of the interval."""
    return self[1]

  @property
  def value_type(self):
    """:obj:`str`: The possible value types of the interval."""
    return ["continuous", "discrete", "ordinal"]