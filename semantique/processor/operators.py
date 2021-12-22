import numpy as np
import xarray as xr

from semantique.processor.templates import TYPE_PROMOTION_TEMPLATES

#
# ALGEBRAIC MULTIVARIATE OPERATORS
#

def add_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.add(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "add", manual = manual)
  return out

def divide_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.divide(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "divide", manual = manual)
  return out

def multiply_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.multiply(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "multiply", manual = manual)
  return out

def power_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.power(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "power", manual = manual)
  return out

def subtract_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.subtract(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "subtract", manual = manual)
  return out

#
# ALGEBRAIC UNIVARIATE OPERATORS
#

def absolute_(x, track_types = False, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.absolute(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, y, func = "absolute", manual = manual)
  return out

def cube_root_(x, track_types = False, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.cbrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, y, func = "cube_root", manual = manual)
  return out

def natural_logarithm_(x, track_types = False, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.log(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, y, func = "natural_logarithm", manual = manual)
  return out

def square_root_(x, track_types = False, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.sqrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, y, func = "square_root", manual = manual)
  return out

#
# BOOLEAN MULTIVARIATE OPERATORS
#

def and_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_and(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "and", manual = manual)
  return out

def or_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_or(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "or", manual = manual)
  return out

def exclusive_or_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_xor(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "exclusive_or", manual = manual)
  return out

#
# BOOLEAN UNIVARIATE OPERATORS
#

def invert_(x, track_types = False, **kwargs):
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.logical_not(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_univariate_operators"]
    out.sq.promote_value_type(x, y, func = "invert", manual = manual)
  return out

#
# EQUALITY OPERATORS
#

def equal_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "equal", manual = manual)
  return out

def in_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "in", manual = manual)
  return out

def not_equal_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.not_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "not_equal", manual = manual)
  return out

def not_in_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y, invert = True), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "not_in", manual = manual)
  return out

#
# REGULAR RELATIONAL OPERATORS
#

def greater_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "greater", manual = manual)
  return out

def greater_equal_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "greater_equal", manual = manual)
  return out

def less_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "less", manual = manual)
  return out

def less_equal_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "less_equal", manual = manual)
  return out

#
# TEMPORAL RELATIONAL OPERATORS
#

def after_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, np.max(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "after", manual = manual)
  return out

def before_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, np.min(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "before", manual = manual)
  return out

def during_(x, y, track_types = False, **kwargs):
  def f(x, y, **kwargs):
    a = np.greater_equal(x, np.min(y))
    b = np.less_equal(x, np.max(y))
    return np.where(np.isfinite(x), np.logical_and(a, b), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "during", manual = manual)
  return out
