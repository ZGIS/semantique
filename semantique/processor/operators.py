import numpy as np
import xarray as xr

from semantique.processor.templates import TYPE_PROMOTION_TEMPLATES

TYPE_PROMOTIONS = {
  "add_": TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"],
  "divide_": TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"],
  "multiply_": TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"],
  "power_": TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"],
  "subtract_": TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"],
  "absolute_": TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"],
  "cube_root_": TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"],
  "natural_logarithm_": TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"],
  "square_root_": TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"],
  "and_": TYPE_PROMOTION_TEMPLATES["logical_multivariate_operators"],
  "or_": TYPE_PROMOTION_TEMPLATES["logical_multivariate_operators"],
  "invert_": TYPE_PROMOTION_TEMPLATES["logical_univariate_operators"],
  "equal_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "exclusive_or_": TYPE_PROMOTION_TEMPLATES["logical_multivariate_operators"],
  "greater_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "greater_equal_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "in_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "less_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "less_equal_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "not_equal_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "not_in_": TYPE_PROMOTION_TEMPLATES["comparison_operators"],
  "after_": TYPE_PROMOTION_TEMPLATES["temporal_operators"],
  "before_": TYPE_PROMOTION_TEMPLATES["temporal_operators"],
  "during_": TYPE_PROMOTION_TEMPLATES["temporal_operators"]
}

#
# ALGEBRAIC MULTIVARIATE OPERATORS
#

def add_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.add(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out

def divide_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.divide(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out

def multiply_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.multiply(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def power_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.power(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def subtract_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.subtract(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

#
# ALGEBRAIC UNIVARIATE OPERATORS
#

def absolute_(x, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.absolute(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  return out

def cube_root_(x, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.cbrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  return out

def natural_logarithm_(x, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.log(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  return out

def square_root_(x, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.sqrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  return out

#
# LOGICAL MULTIVARIATE OPERATORS
#

def and_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_and(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out

def or_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_or(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

#
# LOGICAL UNIVARIATE OPERATORS
#

def invert_(x, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.logical_not(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  return out

#
# COMPARSION OPERATORS
#

def equal_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def exclusive_or_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_xor(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def greater_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def greater_equal_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def in_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def less_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def less_equal_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def not_equal_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.not_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

def not_in_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y, invert = True), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return(out)

#
# TEMPORAL OPERATORS
#

def after_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, np.max(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out

def before_(x, y, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, np.min(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out

def during_(x, y, **kwargs):
  def f(x, y, **kwargs):
    a = np.greater_equal(x, np.min(y))
    b = np.less_equal(x, np.max(y))
    return np.where(np.isfinite(x), np.logical_and(a, b), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  return out
