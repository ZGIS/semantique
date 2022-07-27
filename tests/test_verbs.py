import unittest

import semantique as sq
import numpy as np
import xarray as xr

from xarray import testing

o = np.nan

class TestExtract(unittest.TestCase):

  def test_dimension(self):
    x = xr.DataArray([[1, 2], [3, 4]], coords = {"foo": ["a", "b"], "bar": [0, 1]})
    f = xr.DataArray(["a", "b"], coords = {"foo": ["a", "b"]})
    g = xr.DataArray([0, 1], coords = {"bar": [0, 1]})
    self.assertIsNone(testing.assert_equal(x.sq.extract("foo"), f))
    self.assertIsNone(testing.assert_equal(x.sq.extract("bar"), g))


class TestFilter(unittest.TestCase):

  def test_regular(self):
    x = xr.DataArray([[1, 2], [3, 4]])
    y = xr.DataArray([[1, 0], [0, 1]])
    f = xr.DataArray([[1, o], [o, 4]])
    self.assertIsNone(testing.assert_equal(x.sq.filter(y), f))

  def test_align(self):
    x = xr.DataArray([[1, 2], [3, 4]])
    y = xr.DataArray([1, 0])
    f = xr.DataArray([[1, 2], [o, o]])
    self.assertIsNone(testing.assert_equal(x.sq.filter(y), f))

  def test_self(self):
    x = xr.DataArray([[1, 0], [0, 1]])
    f = xr.DataArray([[1, o], [o, 1]])
    self.assertIsNone(testing.assert_equal(x.sq.filter(x), f))

if __name__ == "__main__":
  unittest.main()