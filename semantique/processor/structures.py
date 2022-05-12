import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

import copy
import pytz
import rasterio
import rioxarray
import warnings

from semantique import exceptions
from semantique.processor import utils

@xr.register_dataarray_accessor("sq")
class Cube():
  """Internal representation of a data cube.

  This data structure is modelled as an accessor of :class:`xarray.DataArray`.
  Using accessors instead of the common class inheritance is recommended by the
  developers of xarray, see `here`_. In practice, this means that each method
  of this class can be called as method of :obj:`xarray.DataArray` objects by
  using the ``.sq`` prefix: ::

    xarray_obj.sq.method

  Parameters
  ----------
    xarray_obj : :obj:`xarray.DataArray`
      The content of the data cube.

  .. _here:
    https://xarray.pydata.org/en/stable/internals/extending-xarray.html

  """

  def __init__(self, xarray_obj):
    self._obj = xarray_obj

  @property
  def value_type(self):
    """:obj:`str`: The value type of the data cube.

    Valid options are: ::

      'numerical', 'nominal', 'ordinal', 'boolean'

    """
    try:
      return self._obj.attrs["value_type"]
    except KeyError:
      return None

  @value_type.setter
  def value_type(self, value):
    self._obj.attrs["value_type"] = value

  @value_type.deleter
  def value_type(self):
    try:
      del self._obj.attrs["value_type"]
    except KeyError:
      pass

  @property
  def value_labels(self):
    """:obj:`dict`: Character labels of data values."""
    try:
      return self._obj.attrs["value_labels"]
    except KeyError:
      return None

  @value_labels.setter
  def value_labels(self, value):
    self._obj.attrs["value_labels"] = value

  @value_labels.deleter
  def value_labels(self):
    try:
      del self._obj.attrs["value_labels"]
    except KeyError:
      pass

  @property
  def crs(self):
    """:obj:`pyproj.crs.CRS`: Coordinate reference system in which the spatial
    coordinates of the cube are expressed."""
    return self._obj.rio.crs

  @property
  def spatial_resolution(self):
    """:obj:`list`: Spatial resolution of the cube in units of the CRS."""
    return self._obj.sq.unstack_spatial_dims().rio.resolution()[::-1]

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Time zone in which the temporal coordinates of
    the cube are expressed."""
    try:
      return pytz.timezone(self._obj["temporal_ref"].attrs["zone"])
    except KeyError:
      return None

  @property
  def is_empty(self):
    """:obj:`bool`: Is the data cube empty."""
    return self._obj.values.size == 0 or not np.any(np.isfinite(self._obj))

  @ property
  def temporal_dimension(self):
    """:obj:`str`: Name of the temporal dimension of the cube."""
    if "time" in self._obj.dims:
      return "time"
    else:
      return None

  @property
  def spatial_dimension(self):
    """:obj:`str`: Name of the spatial dimension of the cube."""
    if "space" in self._obj.dims:
      return "space"
    else:
      return None

  @property
  def xy_dimensions(self):
    """:obj:`list`: Names of respectively the X and Y dimensions of the cube."""
    spatial_dim = self.spatial_dimension
    if spatial_dim is None:
      all_dims = self._obj.dims
    else:
      all_dims = self.extract(spatial_dim).unstack().dims
    candidates = [
      ["x", "y"],
      ["X", "Y"],
      ["longitude", "latitude"],
      ["lon", "lat"],
    ]
    for pair in candidates:
      if all([dim in all_dims for dim in pair]):
        return pair
    return None

  @property
  def grid_points(self):
    """:obj:`geopandas.GeoSeries`: Spatial grid points of the cube."""
    # Extract names of spatial dimensions.
    space_dim = self.spatial_dimension
    if space_dim is None:
      return None
    xy_dims = self.xy_dimensions
    # Extract spatial coordinates.
    xcoords = self._obj[space_dim][xy_dims[0]]
    ycoords = self._obj[space_dim][xy_dims[1]]
    # Return grid points as geometries.
    points = gpd.points_from_xy(xcoords, ycoords)
    return gpd.GeoSeries(points, crs = self.crs)

  def evaluate(self, operator, y = None, track_types = True, **kwargs):
    """Apply the evaluate verb to the cube.

    The evaluate verb evaluates an expression for each pixel in a data cube.

    Parameters
    ----------
    operator : :obj:`callable`
      Operator function to be used in the expression.
    y : optional
      Right-hand side of the expression. May be a constant, meaning that the
      same value is used in each expression. May also be another data cube
      which can be aligned to the same shape as the input cube. In the latter
      case, when evaluating the expression for a pixel in the input cube the
      second operand is the value of the pixel in cube ``y`` that has the same
      dimension coordinates. Ignored when the operator is univariate.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type(s) of the operand(s)?
    **kwargs:
      Additional keyword arguments passed on to the operator function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    operands = tuple([self._obj]) if y is None else tuple([self._obj, y])
    out = operator(*operands, track_types = track_types, **kwargs)
    return out

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to the cube.

    The extract verb extracts coordinate labels of a dimension as a new data
    cube.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to be extracted.
      component : :obj:`str`, optional
        Name of a specific component of the dimension coordinates to be
        extracted, e.g. *year*, *month* or *day* for temporal dimension
        coordinates.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the data cube.
      :obj:`exceptions.UnknownComponentError`
        If the given dimension does not contain the given component.

    """
    try:
      coords = self._obj[dimension]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the input object"
      )
    if component is None:
      out = coords
    else:
      try:
        out = coords[component]
      except KeyError:
        try:
          out = getattr(coords.dt, component)
        except AttributeError:
          raise exceptions.UnknownComponentError(
            f"Component '{component}' "
            f"is not defined for dimension '{dimension}'"
          )
        else:
          out = utils.parse_datetime_component(component, out)
      else:
        try:
          if component in self.xy_dimensions:
            out = utils.parse_coords_component(out)
        except TypeError:
          pass
    out._variable = out._variable.to_base_variable()
    return out

  def filter(self, filterer, trim = False, track_types = True, **kwargs):
    """Apply the filter verb to the cube.

    The filter verb filters the values in a data cube.

    Parameters
    -----------
      filterer : :obj:`xarray.DataArray`
        Data cube which can be aligned to the same shape as the input cube.
        Each pixel in the input cube will be kept if the pixel in the
        filterer with the same dimension coordinates has true as value, and
        dropped otherwise.
      trim : :obj:`bool`
        Should the filtered cube be trimmed before returning?
        Trimming means that all coordinates for which all values are nodata, are
        dropped from the array. The spatial dimension (if present) is treated
        differently, by trimming it only at the edges, and thus maintaining the
        regularity of the spatial dimension.
      track_types : :obj:`bool`
        Should it be checked if the filterer has value type *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of ``filterer`` is not
        *binary*.

    """
    if track_types:
      vtype = filterer.sq.value_type
      if vtype is not None and vtype != "binary":
        raise exceptions.InvalidValueTypeError(
          f"Filterer must be of value type 'binary', not '{vtype}'"
        )
    out = self._obj.where(filterer.sq.align_with(self._obj))
    if trim:
      out = out.sq.trim()
    return out

  def groupby(self, grouper, **kwargs):
    """Apply the groupby verb to the cube.

    The groupby verb groups the values in a data cube.

    Parameters
    -----------
      grouper : :obj:`xarray.DataArray` or :obj:`CubeCollection`
        Data cube containing a single dimension that is also present in the
        input cube. The group to which each pixel in the input cube will be
        assigned depends on the value of the grouper that has the same
        coordinate for that dimension. Alternatively it may be a cube
        collection in which each cube meets the requirements above. In that
        case, groups are defined by the unique combinations of corresponding
        values in all collection members.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`CubeCollection`

    Raises
    -------
      :obj:`exceptions.MissingDimensionError`
        If the grouper dimension is not present in the input object.
      :obj:`exceptions.TooManyDimensionsError`
        If the grouper has more than one dimension.
      :obj:`exceptions.MixedDimensionsError`
        If the grouper is a collection and its elements don't all have the same
        dimensions.

    """
    # Validate grouper.
    if isinstance(grouper, list):
      multiple = True
      dims = [x.dims for x in grouper]
    else:
      multiple = False
      dims = [grouper.dims]
    if not all([len(x) == 1 for x in dims]):
      raise exceptions.TooManyDimensionsError(
        "Groupers must be one-dimensional"
      )
    if not all([x == dims[0] for x in dims]):
      raise exceptions.MixedDimensionsError(
        "Dimensions of grouper arrays do not match"
      )
    if not dims[0][0] in self._obj.dims:
      raise exceptions.MissingDimensionError(
        f"Grouper dimension '{dims[0]}' does not exist in the input object"
      )
    # Split input object into groups.
    if multiple:
      idx = pd.MultiIndex.from_arrays([x.data for x in grouper])
      dim = grouper[0].dims
      partition = list(self._obj.groupby(xr.IndexVariable(dim, idx)))
    else:
      partition = list(self._obj.groupby(grouper))
    out = CubeCollection([i[1].sq.label(i[0]) for i in partition])
    return out

  def label(self, label, **kwargs):
    """Apply the label verb to the cube.

    The label verb labels a data cube with a word or phrase.

    Parameters
    -----------
      label : :obj:`str`
        Character label to be attached to the input cube.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj.rename(label)
    return out

  def reduce(self, dimension, reducer, track_types = True, **kwargs):
    """Apply the reduce verb to the cube.

    The reduce verb reduces the dimensionality of a data cube.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to apply the reduction function to.
      operator : :obj:`callable`
        The reducer function to be applied.
      track_types : :obj:`bool`
        Should the reducer promote the value type of the output object, based
        on the value type of the input object?
      **kwargs:
        Additional keyword arguments passed on to the reducer function.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the data cube.

    """
    if dimension not in self._obj.dims:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the input object"
      )
    out = reducer(self._obj, dimension, track_types = track_types, **kwargs)
    return out

  def align_with(self, other):
    """Align the cube to the shape of another data cube.

    An input cube is alinged to another cube if the pixel at position *i* in
    the input cube has the same coordinates as the pixel at position *i* in the
    other cube. Aligning can be done in several ways:

    * Consider the case where the input cube has exactly the same dimensions
      and coordinates as the other cube, but the order of them is different.
      In that case, the input cube is simply re-ordered to match the other
      cube.

    * Consider the case where the input cube has the same dimensions as the
      other cube, but not all coordinates match. In that case, the coordinates
      that are in the input cube but not in the other cube are removed from the
      input cube, and at the same time the coordinates that are in the other
      cube but not in the input cube are added to the input cube.

    * Consider the case where all dimensions of the input cube are also present
      in the other cube, but not all dimensions of the other cube are present
      in the input cube. In that case, the pixels of the input cube are
      duplicated along those dimensions that are missing.

    Alignment may also be a combination of more than one of these ways.

    Parameters
    -----------
      other : :obj:`xarray.DataArray`
        Data cube to which the input cube should be aligned.

    Returns
    --------
      :obj:`xarray.DataArray`
        The aligned input cube.

    Raises
    -------
      :obj:`exceptions.AlignmentError`
        If the input cube cannot be aligned to the other cube, for example when
        the two cubes have no dimensions in common at all, or when the input
        cube has dimensions that are not present in the other cube.

    """
    out = xr.align(other, self._obj, join = "left")[1].broadcast_like(other)
    if not out.shape == other.shape:
      raise exceptions.AlignmentError(
        f"Cube '{other.name if other.name is not None else 'y'}' "
        f"cannot be aligned with "
        f"input cube '{self._obj.name if self._obj.name is not None else 'y'}'"
      )
    return out

  def trim(self, force_regular = True):
    """Trim the dimensions of the cube.

    Trimming means that all coordinates for which all values are nodata, are
    dropped from the array.

    Parameters
    ----------
      force_regular : :obj:`bool`
        If spatial dimensions are present, should the regularity of these
        dimensions be preserved? If ``True`` the spatial dimensions are
        trimmed only at their edges.

    Returns
    -------
      :obj:`xarray.DataArray`
        The trimmed input cube.

    """
    space = self.spatial_dimension
    if force_regular and space is not None:
      # Trim all dimensions normally except the spatial dimension.
      out = self._obj
      all_dims = out.dims
      trim_dims = [d for d in all_dims if d != space]
      # Trim spatial dimensions separately while preserving regularity.
      # Hence trimming only at the edges of the spatial dimensions.
      # First unstack spatial dimension into x and y dimensions.
      out = out.unstack(space)
      xy_dims = self.xy_dimensions
      y_dim = xy_dims[1]
      x_dim = xy_dims[0]
      # For the x and y dimensions:
      # Find the smallest and largest coordinates containing valid values.
      y_idxs = np.nonzero(out.count(trim_dims + [x_dim]).data)[0]
      x_idxs = np.nonzero(out.count(trim_dims + [y_dim]).data)[0]
      y_slice = slice(y_idxs.min(), y_idxs.max() + 1)
      x_slice = slice(x_idxs.min(), x_idxs.max() + 1)
      # Limit the x and y coordinates to only those ranges.
      out = out.isel({y_dim: y_slice, x_dim: x_slice})
      # Stack x and y dimensions back together.
      out = out.stack({space: [y_dim, x_dim]})
      out[space].sq.value_type = "coords"
    else:
      # Trim all dimensions normally.
      out = self._obj
      all_dims = out.dims
      trim_dims = all_dims
    # Apply normal trimming to the selected dimensions.
    for dim in trim_dims:
      other_dims = [d for d in all_dims if d != dim]
      out = out.isel({dim: out.count(other_dims) > 0})
    return out

  def regularize(self):
    """Regularize the spatial dimension of the cube.

    Regularizing makes sure that the steps between subsequent coordinates of
    the spatial dimensions are always equal to the resolution of that
    dimensions.

    Returns
    -------
      :obj:`xarray.DataArray`
        The regularized input cube.

    """
    space = self.spatial_dimension
    if space is None:
      return self._obj
    obj = self._obj.unstack(space)
    res = self.spatial_resolution
    # Extract x and y dimensions.
    xydims = self.xy_dimensions
    yname = xydims[1]
    ycoords = obj[yname]
    xname = xydims[0]
    xcoords = obj[xname]
    # Update x and y dimensions.
    ycoords = np.arange(ycoords[0], ycoords[-1] + res[0], res[0])
    xcoords = np.arange(xcoords[0], xcoords[-1] + res[1], res[1])
    out = obj.reindex({yname: ycoords, xname: xcoords})
    return out.stack({space: [yname, xname]})

  def reproject(self, crs, **kwargs):
    """Reproject the spatial coordinates of the cube into a different CRS.

    Parameters
    ----------
      crs:
        Target coordinate reference system. Can be given as any object
        understood by the initializer of :class:`pyproj.crs.CRS`. This includes
        :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG codes and WKT
        strings.
      **kwargs:
        Additional keyword arguments passed on to
        :meth:`rioxarray.rioxarray.XRasterBase.reproject`.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input cube with reprojected spatial coordinates.

    """
    space = self.spatial_dimension
    if space is None:
      return self._obj
    obj = self.unstack(space)
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    out = obj.rio.reproject(crs, **kwargs)
    out = out.sq.stack_spatial_dims(name = space).sq.write_tz(self.tz)
    return out

  def tz_convert(self, tz, **kwargs):
    """Convert the temporal coordinates of the cube into a different timezone.

    Parameters
    ----------
      tz:
        Target timezone. Can be given as :obj:`str` referring to the name of a
        timezone in the tz database, or as instance of any class inheriting
        from :class:`datetime.tzinfo`.
      **kwargs:
        Additional keyword arguments passed on to
        :func:`convert_datetime64 <semantique.processor.utils.convert_datetime64>`.

    Returns
    -------
      :obj:`xarray.DataArray`
        The input cube with converted temporal coordinates.

    """
    time = self.temporal_dimension
    if time is None:
      return self._obj
    src = self._obj[time].data
    trg = [utils.convert_datetime64(x, self.tz, tz, **kwargs) for x in src]
    out = self._obj.assign_coords({time: trg}).sq.write_tz(tz)
    return out

  def write_crs(self, crs, inplace = False):
    """Store the CRS of the cube as non-dimension coordinate.

    The coordinate reference system of the spatial coordinates is stored as
    attribute of a specific non-dimension coordinate named "spatial_ref".
    Storing this inside a non-dimension coordinate rather than as direct
    attribute of the array guarantees that this information is preserved
    during any kind of operation. The coordinate itself serves merely as a
    placeholder.

    Parameters
    ----------
      crs:
        The spatial coordinate reference system to store. Can be given as any
        object understood by the initializer of :class:`pyproj.crs.CRS`. This
        includes :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG
        codes and WKT strings.
      inplace : :obj:`bool`
        Should the cube be modified inplace?

    Returns
    -------
      :obj:`xarray.DataArray`
        The input cube with the CRS stored in a non-dimension coordinate.

    """
    return self._obj.rio.write_crs(crs, inplace = inplace)

  def write_tz(self, tz, inplace = False):
    """Store the timezone of the cube as non-dimension coordinate.

    The timezone of the temporal coordinates is stored as attribute of a
    specific non-dimension coordinate named "temporal_ref". Storing this inside
    a non-dimension coordinate rather than as direct attribute of the array
    guarantees that this information is preserved during any kind of operation.
    The coordinate itself serves merely as a placeholder.

    Parameters
    ----------
      tz:
        The timezone to store. Can be given as :obj:`str` referring to the name
        of a timezone in the tz database, or as instance of any class
        inheriting from :class:`datetime.tzinfo`.
      inplace : :obj:`bool`
        Should the cube be modified inplace?

    Returns
    -------
      :obj:`xarray.DataArray`
        The input cube with the timezone stored in a non-dimension coordinate.

    """
    obj = self._obj if inplace else copy.deepcopy(self._obj)
    try:
      zone = tz.zone
    except AttributeError:
      zone = pytz.timezone(tz).zone
    obj["temporal_ref"] = 0
    obj["temporal_ref"].attrs["zone"] = zone
    return obj

  def stack_spatial_dims(self, name = "space"):
    """Stack the spatial X and Y dimensions into a single spatial dimension.

    Parameters
    -----------
    name : :obj:`str`
      Name that should be given to the stacked dimension.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input cube with stacked spatial dimensions.

    """
    xy_dims = self.xy_dimensions
    if xy_dims is None:
      return self._obj
    return self._obj.stack({name: xy_dims[::-1]})

  def unstack_spatial_dims(self):
    """Unstack the spatial dimension into separate X and Y dimensions.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input cube with unstacked spatial dimensions.

    """
    dim = self.spatial_dimension
    if dim is None:
      return self._obj
    return self._obj.unstack(dim)

  def drop_non_dimension_coords(self, keep = None):
    """Drop non-dimension coordinates from the cube.

    Non-dimension coordinates are coordinates that are used for e.g. auxilary
    labeling or metadata storage. See the `xarray documentation`_.

    Parameters
    -----------
      keep : :obj:`list`, optional
        List of non-dimension coordinate names that should not be dropped.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input cube without non-dimension coordinates

    .. _xarray documentation:
      https://xarray.pydata.org/en/stable/user-guide/terminology.html#term-Non-dimension-coordinate

    """
    if keep is None:
      drop = set(self._obj.coords) - set(self._obj.dims)
    else:
      drop = set(self._obj.coords) - set(self._obj.dims) - set(keep)
    return self._obj.reset_coords(drop, drop = True)

  def to_dataframe(self):
    """Convert the cube to a pandas DataFrame.

    The data frame will contain one column per dimension, and a column
    containing the data values.

    Returns
    -------
      :obj:`pandas.DataFrame`
        The converted input cube

    """
    obj = self.drop_non_dimension_coords().sq.unstack_spatial_dims()
    # to_dataframe method does not work for zero-dimensional arrays.
    if len(self._obj.dims) == 0:
      out = pd.DataFrame([obj.values])
    else:
      out = obj.to_dataframe()
    return out

  def to_geodataframe(self, output_crs = None):
    """Convert the cube to a geopandas GeoDataFrame.

    The data frame will contain one column per dimension, a column
    containing the data values, and a geometry column containing coordinates of
    geospatial points that represent the centroids of the pixels in the cube.

    Parameters
    ----------
      output_crs : optional
        Spatial coordinate reference system of the GeoDataFrame. Can be
        given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the cube itself is used.

    Returns
    -------
      :obj:`geopandas.GeoDataFrame`
        The converted input cube

    Raises
    ------
      :obj:`exceptions.MissingDimensionError`
        If the cube does not have spatial dimensions.

    """
    # Make sure spatial dimensions are present.
    spatial_dims = self.xy_dimensions
    if spatial_dims is None:
      raise exceptions.MissingDimensionError(
        "GeoDataFrame conversion requires spatial dimensions"
      )
    # Convert to dataframe.
    df = self.to_dataframe().reset_index()
    # Create geometries.
    geoms = gpd.points_from_xy(df[spatial_dims[0]], df[spatial_dims[1]])
    # Convert to geodataframe
    gdf = gpd.GeoDataFrame(df, geometry = geoms, crs = self.crs)
    # Reproject if needed.
    if output_crs is not None:
      gdf = gdf.to_crs(output_crs)
    return gdf

  def to_csv(self, file):
    """Write the content of the cube to a CSV file on disk.

    The CSV file will contain one column per dimension, and a column containing
    the data values.

    Parameters
    ----------
      file : :obj:`str`
        Path to the CSV file to be written.

    Returns
    --------
      :obj:`str`
        Path to the written CSV file.

    """
    df = self.to_dataframe(unstack = unstack)
    if len(self._obj.dims) == 0:
      df.to_csv(file, header = False, index = False)
    else:
      df.to_csv(file)
    return file

  def to_geotiff(self, file, cloud_optimized = True, compress = True,
                 output_crs = None):
    """Write the content of the cube to a GeoTIFF file on disk.

    Parameters
    ----------
      file : :obj:`str`
        Path to the GeoTIFF file to be written.
      cloud_optimized : :obj:`bool`
        Should the written file be a Cloud Optimized GeoTIFF (COG) instead of a
        regular GeoTIFF? Ignored when a GDAL version < 3.1 is used. These GDAL
        versions do not have a COG driver, and the written GeoTIFF will always
        be a regular GeoTIFF.
      compress : :obj:`bool`
        Should the written file be compressed? If ``True``, LZW compression is
        used.
      output_crs : optional
        Spatial coordinate reference system of the written GeoTIFF. Can be
        given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the cube itself is used.

    Returns
    -------
      :obj:`str`
        Path to the written GeoTIFF file.

    Raises
    ------
      :obj:`exceptions.MissingDimensionError`
        If the cube does not have spatial dimensions.
      :obj:`exceptions.TooManyDimensionsError`
        If the cube has more than three dimensions, including the two unstacked
        spatial dimensions. More than three dimensions is currently not
        supported by the export functionality of rasterio.

    """
    # Make sure spatial dimensions are present.
    if self.xy_dimensions is None:
      raise exceptions.MissingDimensionError(
        "GeoTIFF export requires spatial dimensions"
      )
    obj = self.unstack_spatial_dims()
    # Remove non-dimension coordinates but not 'spatial_ref'.
    # That one is needed by rioxarray to determine the CRS of the data.
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    # Reproject data if requested.
    if output_crs is not None:
      obj = obj.rio.reproject(output_crs)
    # Initialize GDAL configuration parameters.
    config = {}
    # GDAL has limited support for numpy dtypes.
    # Therefore dtype conversion might be needed in some cases.
    dtype = obj.dtype
    if not rasterio.dtypes.check_dtype(dtype):
      dtype = rasterio.dtypes.get_minimum_dtype(obj)
    config["dtype"] = dtype
    # GDAL support COG export only since version 3.1.
    if cloud_optimized:
      if rasterio.__gdal_version__ < "3.1":
        config["driver"] = "GTiff"
        warnings.warn(
          f"Cloud optimized export is not supported by GDAL version "
          f"{rasterio.__gdal_version__}, written to regular GeoTIFF instead"
        )
      else:
        config["driver"] = "COG"
    else:
      config["driver"] = "GTiff"
    # Use LZW compression if compression is requested by user.
    if compress:
      config["compress"] = "LZW"
    # Write data to file.
    try:
      obj.rio.to_raster(file, **config)
    except rioxarray.exceptions.TooManyDimensions:
      ndims = len(obj.dims) - 2
      raise exceptions.TooManyDimensionsError(
        f"GeoTIFF export is only supported for 2D or 3D arrays, not {ndims}D"
      )
    return file

class CubeCollection(list):
  """Internal representation of a data cube collection.

  Parameters
  ----------
    list_obj : :obj:`list` of :obj:`xarray.DataArray`
      The elements of the data cube collection stored in a list.

  """

  def __init__(self, list_obj):
    super(CubeCollection, self).__init__(list_obj)

  @property
  def is_empty(self):
    """:obj:`bool`: Are all elements of the collection empty data cubes."""
    return all([x.sq.is_empty for x in self])

  def compose(self, track_types = True, **kwargs):
    """Apply the compose verb to the collection.

    The compose verb creates a categorical composition from the cubes in the
    collection.

    Parameters
    -----------
      track_types : :obj:`bool`
        Should it be checked if all cubes in the collection have value type
        *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of at least one of the
        cubes in the collection is not *binary*.

    """
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == "binary" for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'compose' should all be 'binary', "
          f"not {np.unique(value_types).tolist()} "
        )
    def index_(idx, obj):
      return xr.where(obj, idx + 1, np.nan).where(obj.notnull())
    enumerated = enumerate(self)
    indexed = [index_(i, x) for i, x in enumerated]
    out = indexed[0]
    for x in indexed[1:]:
      out = out.combine_first(x)
    labels = [x.name for x in self]
    idxs = range(1, len(labels) + 1)
    out.sq.value_type = "nominal"
    out.sq.value_labels = {k:v for k, v in zip(idxs, labels)}
    return out

  def concatenate(self, dimension, track_types = True,
                  vtype = "nominal", **kwargs):
    """Apply the concatenate verb to the collection.

    The concatenate verb concatenates the cubes in the collection along a new
    or existing dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to concatenate along. To concatenate along an
        existing dimension, it should be a dimension that exists in all
        collection members. To concatenate along a new dimension, it should be
        a dimension that does not exist in any of the collection members.
      track_types : :obj:`bool`
        Should it be checked if all cubes in the collection have the same value
        type?
      vtype : :obj:`str`:
        If the cubes are concatenated along a new dimension, what should the
        value type of its dimension coordinates be? Valid options are
        "numerical", "nominal", "ordinal" and "boolean".
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value types of the cubes in the
        collection are not all equal to each other.

    """
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == value_types[0] for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'concatenate' should all be the same, "
          f"not {np.unique(value_types).tolist()} "
        )
    if dimension in self[0].dims:
      # Concatenate over existing dimension.
      raw = xr.concat([x for x in self], dimension)
      coords = raw.get_index(dimension)
      clean = raw.isel({dimension: np.invert(coords.duplicated())})
      out = clean.sortby(dimension)
    else:
      # Concatenate over new dimension.
      labels = [x.name for x in self]
      coords = pd.Index(labels, name = dimension, tupleize_cols = False)
      out = xr.concat([x for x in self], coords)
      out[dimension].sq.value_type = vtype
      out[dimension].sq.value_labels = {x:x for x in labels}
    # PROVISIONAL FIX: Always drop value labels of the concatenated cube.
    # Value labels are preserved from the first element in the collection.
    # These may not be accurate anymore for the concatenated cube.
    # TODO: Find a way to merge value labels. But how to handle duplicates?
    del out.sq.value_labels
    return out

  def merge(self, reducer, track_types = True, **kwargs):
    """Apply the merge verb to the collection.

    The merge verb merges the pixel values of all cubes in the collection into
    a single value per pixel.

    Parameters
    -----------
      reducer : :obj:`str`
        Name of the reducer function to be applied in order to reduce multiple
        values per pixel into a single value. Should either be one of the
        built-in reducers of semantique, or a user-defined reducer which will
        be provided to the query processor when executing the query recipe.
      track_types : :obj:`bool`
        Should it be checked if all cubes in the collection have the same value
        type, and should the reducer promote the value type of the output
        object, based on the value type of the input objects?
      **kwargs:
        Additional keyword arguments passed on to the reducer function.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value types of the cubes in the
        collection are not all equal to each other.

    """
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == value_types[0] for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'merge' should all be the same, "
          f"not {np.unique(value_types).tolist()} "
        )
    dim = "__sq__" # Temporary dimension.
    concat = self.concatenate(dim, track_types = False)
    out = concat.sq.reduce(dim, reducer, track_types, **kwargs)
    return out

  def evaluate(self, operator, y = None, track_types = True, **kwargs):
    """Apply the evaluate verb to all cubes in the collection.

    See :meth:`Cube.evaluate`

    Returns
    -------
      :obj:`CubeCollection`

    """
    args = tuple([operator, y, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.evaluate(*args, **kwargs) for x in out]
    return out

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to all cubes in the collection.

    See :meth:`Cube.extract`

    Returns
    -------
      :obj:`CubeCollection`

    """
    args = tuple([dimension, component])
    out = copy.deepcopy(self)
    out[:] = [x.sq.extract(*args, **kwargs) for x in out]
    return out

  def filter(self, filterer, trim = True, track_types = True, **kwargs):
    """Apply the filter verb to all cubes in the collection.

    See :meth:`Cube.filter`

    Returns
    -------
      :obj:`CubeCollection`

    """
    args = tuple([filterer, trim, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.filter(*args, **kwargs) for x in out]
    return out

  def label(self, label, **kwargs):
    """Apply the label verb to all cubes in the collection.

    See :meth:`Cube.label`

    Returns
    -------
      :obj:`CubeCollection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.label(label, **kwargs) for x in out]
    return out

  def reduce(self, dimension, reducer, track_types = True, **kwargs):
    """Apply the reduce verb to all cubes in the collection.

    See :meth:`Cube.reduce`

    Returns
    -------
      :obj:`CubeCollection`

    """
    args = tuple([dimension, reducer, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.reduce(*args, **kwargs) for x in out]
    return out

  def trim(self, force_regular = True):
    """Trim the dimensions of all cubes in the collection.

    See :meth:`Cube.trim`

    Returns
    -------
      :obj:`CubeCollection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.trim(force_regular) for x in out]
    return out

  def regularize(self):
    """Regularize the spatial dimension of all cubes in the collection.

    See :meth:`Cube.regularize`

    Returns
    -------
      :obj:`CubeCollection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.regularize() for x in out]
    return out

  def stack_spatial_dims(self, name = "space"):
    """Stack the spatial dimensions for all cubes in the collection.

    See :meth:`Cube.stack_spatial_dims`

    Returns
    -------
      :obj:`CubeCollection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.stack_spatial_dims(name) for x in out]
    return out

  def unstack_spatial_dims(self):
    """Unstack the spatial dimensions for all cubes in the collection.

    See :meth:`Cube.unstack_spatial_dims`

    Returns
    -------
      :obj:`CubeCollection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.unstack_spatial_dims() for x in out]
    return out