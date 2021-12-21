import numpy as np
from scipy import stats

from semantique.processor.templates import TYPE_PROMOTION_TEMPLATES

TYPE_PROMOTIONS = {
  "mean_": TYPE_PROMOTION_TEMPLATES["numerical_reducers"],
  "product_": TYPE_PROMOTION_TEMPLATES["numerical_reducers"],
  "standard_deviation_": TYPE_PROMOTION_TEMPLATES["numerical_reducers"],
  "sum_": TYPE_PROMOTION_TEMPLATES["numerical_reducers"],
  "variance_": TYPE_PROMOTION_TEMPLATES["numerical_reducers"],
  "all_": TYPE_PROMOTION_TEMPLATES["boolean_reducers"],
  "any_": TYPE_PROMOTION_TEMPLATES["boolean_reducers"],
  "count_": TYPE_PROMOTION_TEMPLATES["count_reducers"],
  "percentage_": TYPE_PROMOTION_TEMPLATES["count_reducers"],
  "max_": TYPE_PROMOTION_TEMPLATES["ordered_reducers"],
  "median_": TYPE_PROMOTION_TEMPLATES["ordered_reducers"],
  "min_": TYPE_PROMOTION_TEMPLATES["ordered_reducers"],
  "first_": TYPE_PROMOTION_TEMPLATES["universal_reducers"],
  "last_": TYPE_PROMOTION_TEMPLATES["universal_reducers"],
  "mode_": TYPE_PROMOTION_TEMPLATES["universal_reducers"]
}

def _is_all_nodata(x, axis):
  return np.equal(np.sum(np.isfinite(x), axis = axis), 0)

def _nodata_as_zero(x):
  return np.nan_to_num(x, nan = 0, posinf = 0, neginf = 0)

#
# NUMERICAL REDUCERS
#

def mean_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanmean(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def product_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    values = np.nanprod(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def standard_deviation_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanstd(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def sum_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    values = np.nansum(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def variance_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanvar(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

#
# BOOLEAN REDUCERS
#

def all_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    values = np.all(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def any_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.any(_nodata_as_zero(x), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

#
# COUNT REDUCERS
#

def count_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    values = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def percentage_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    part = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    part = np.where(_is_all_nodata(x, axis), np.nan, part)
    whole = np.sum(np.isfinite(x), axis = axis)
    return np.multiply(np.divide(part, whole), 100)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

#
# ORDERED REDUCERS
#

def max_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanmax(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def median_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanmedian(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def min_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return np.nanmin(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

#
# UNIVERSAL REDUCERS
#

def first_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    is_value = np.isfinite(x)
    is_last = np.equal(np.cumsum(np.cumsum(is_value, axis = axis), axis = axis), 1)
    return np.nanmax(np.where(is_last, x, np.nan), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def last_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    return first(np.flip(x, axis = axis), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out

def mode_(x, dimension, **kwargs):
  def f(x, axis, **kwargs):
    values = stats.mode(x, axis = axis, nan_policy = "omit")[0].squeeze(axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  return out