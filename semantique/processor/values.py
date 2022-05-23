from semantique import exceptions

class ValueRange(list):
  """Object to represent a range of values.

  A value range in semantique is defined by two values: the value at the start
  of the range and the value at the end of the range. All values in between
  those are part of the range, no matter their precision. The range forms a
  closed interval, meaning that the start and end themselves also belong to the
  range. By definition, the given values should be either :obj:`int` or
  :obj:`float`, and the start should be smaller than or equal to the end.

  Parameters
  ----------
    start : :obj:`int` or :obj:`float`
      The first value in the range.
    end : :obj:`int` or :obj:`float`
      The last value in the range.

  """

  def __init__(self, start, end):
    if end < start:
      raise exceptions.InvalidValueRangeException(
        "The start of value range cannot be smaller than its end"
      )
    super(ValueRange, self).__init__([start, end])

  @property
  def sq(self):
    """Semantique accessor.

    This is merely provided to ensure compatible behaviour with
    :obj:`Cube <semantique.processor.structures.Cube>` objects, which are
    modelled as an accessor to :obj:`xarray.DataArray` objects.

    """
    return self

  @property
  def start(self):
    """:obj:`int` or :obj:`float`: The first value in the range."""
    return self[0]

  @property
  def end(self):
    """:obj:`int` or :obj:`float`: The last value in the range."""
    return self[1]

  @property
  def value_type(self):
    """:obj:`str`: The possible value types of the range."""
    return ["numerical", "ordinal"]
