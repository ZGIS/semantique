import numpy as np
from scipy import stats

from semantique.processor.types import TypePromoter

def _is_all_nodata(x, axis):
  return np.equal(np.sum(np.isfinite(x), axis = axis), 0)

def _nodata_as_zero(x):
  return np.nan_to_num(x, nan = 0, posinf = 0, neginf = 0)

#
# NUMERICAL REDUCERS
#

def mean_(x, dimension, track_types = True, **kwargs):
  """Calculate the mean of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "mean")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmean(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def product_(x, dimension, track_types = True, **kwargs):
  """Calculate the product of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "product")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.nanprod(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def standard_deviation_(x, dimension, track_types = True, **kwargs):
  """Calculate the standard deviation of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "standard_deviation")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanstd(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def sum_(x, dimension, track_types = True, **kwargs):
  """Calculate the sum of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "sum")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.nansum(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def variance_(x, dimension, track_types = True, **kwargs):
  """Calculate the variance of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "variance")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanvar(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# BOOLEAN REDUCERS
#

def all_(x, dimension, track_types = True, **kwargs):
  """Test if all values in a set are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "all")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.all(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def any_(x, dimension, track_types = True, **kwargs):
  """Test if at least one value in a set is true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "any")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.any(_nodata_as_zero(x), axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# COUNT REDUCERS
#

def count_(x, dimension, track_types = True, **kwargs):
  """Count the number of true values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "count")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def percentage_(x, dimension, track_types = True, **kwargs):
  """Calculate the percentage of true values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "percentage")
    promoter.check()
  def f(x, axis, **kwargs):
    part = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    part = np.where(_is_all_nodata(x, axis), np.nan, part)
    whole = np.sum(np.isfinite(x), axis = axis)
    return np.multiply(np.divide(part, whole), 100)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# ORDERED REDUCERS
#

def max_(x, dimension, track_types = True, **kwargs):
  """Return the maximum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "max")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmax(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def median_(x, dimension, track_types = True, **kwargs):
  """Return the median value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "median")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmedian(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def min_(x, dimension, track_types = True, **kwargs):
  """Return the minimum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "min")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmin(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# UNIVERSAL REDUCERS
#

def first_(x, dimension, track_types = True, **kwargs):
  """Return the first value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "first")
    promoter.check()
  def f(x, axis, **kwargs):
    is_value = np.isfinite(x)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis = axis), axis = axis), 1)
    return np.nanmax(np.where(is_first, x, np.nan), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def last_(x, dimension, track_types = True, **kwargs):
  """Return the last value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  """
  if track_types:
    promoter = TypePromoter(x, function = "last")
    promoter.check()
  def f(x, axis, **kwargs):
    xflipped = np.flip(x, axis = axis)
    is_value = np.isfinite(xflipped)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis = axis), axis = axis), 1)
    return np.nanmax(np.where(is_first, xflipped, np.nan), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def mode_(x, dimension, track_types = True, **kwargs):
  """Return the most occuring value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -------
    If there are multiple modal values in a set, the minimum of them (i.e. the
    smallest of these values) is returned.

  """
  if track_types:
    promoter = TypePromoter(x, function = "mode")
    promoter.check()
  def f(x, axis, **kwargs):
    values = stats.mode(x, axis = axis, nan_policy = "omit")[0].squeeze(axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out