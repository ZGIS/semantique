import numpy as np
import xarray as xr

import copy
import datacube
import datetime
import os
import pytz
import rasterio
import rioxarray
import warnings

from datacube.utils import masking
from abc import abstractmethod

from semantique import exceptions
from semantique.dimensions import TIME, SPACE, X, Y

class Datacube():
  """Base class for EO data cube configurations.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout describing the EO data cube. If :obj:`None`, an empty
      EO data cube instance is constructed.

  """

  def __init__(self, layout = None):
    self.layout = layout

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else value

  def lookup(self, *reference):
    """Lookup the metadata of a referenced data layer.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.

    Raises
    -------
      :obj:`exceptions.UnknownLayerError`
        If the referenced data layer does not have a metadata object in the
        layout of the EO data cube.

    """
    obj = self._layout
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.UnknownLayerError(
          f"The EO data cube does not contain layer '{reference}'"
        )
    return obj

  @abstractmethod
  def retrieve(self, *reference, extent):
    """Abstract method for the retriever function.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    """
    pass

class Opendatacube(Datacube):
  """Opendatacube specific EO data cube configuration.

  This class is an interface to Opendatacube backends. See the
  `Opendatacube manual`_ for details.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    connection : :obj:`datacube.Datacube`
      Opendatacube interface object allowing to read from the data cube.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **trim** (:obj:`bool`): Should each retrieved data layer be trimmed?
        Trimming means that dimension coordinates for which all values are
        missing are removed from the array. The spatial dimensions are trimmed
        only at the edges, to maintain their regularity. Defaults to
        :obj:`True`.

      * **group_by_solar_day** (:obj:`bool`): Should the time dimension be
        resampled to the day level, using solar day to keep scenes together?
        Defaults to :obj:`True`.

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the layout map to the value types used by semantique?
        Defaults to a one-to-one mapping: ::

          {
            "nominal": "nominal",
            "ordinal": "ordinal",
            "binary": "binary",
            "continuous": "continuous",
            "discrete": "discrete"
          }

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {
            "nominal": "nearest",
            "ordinal": "nearest",
            "binary": "nearest",
            "continuous": "nearest",
            "discrete": "nearest"
          }

  .. _Opendatacube manual:
    https://datacube-core.readthedocs.io/en/latest/index.html

  """

  def __init__(self, layout = None, connection = None, tz = "UTC", **config):
    super(Opendatacube, self).__init__(layout)
    self.connection = connection
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def connection(self):
    """:obj:`datacube.Datacube`: Opendatacube interface object allowing to read
    from the data cube."""
    return self._connection

  @connection.setter
  def connection(self, value):
    if value is not None:
      assert isinstance(value, datacube.Datacube)
    self._connection = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    if isinstance(value, datetime.tzinfo):
      self._tz = value
    elif isinstance(value, str):
      self._tz = pytz.timezone(value)
    else:
      raise exceptions.UnknownTimeZoneError(
        f"Cannot recognize {value} as a valid timezone specification"
      )

  @property
  def _default_config(self):
    return {
      "trim": True,
      "group_by_solar_day": True,
      "value_type_mapping": {
        "nominal": "nominal",
        "ordinal": "ordinal",
        "binary": "binary",
        "continuous": "continuous",
        "discrete": "discrete"
      },
      "resamplers": {
        "nominal": "nearest",
        "ordinal": "nearest",
        "binary": "nearest",
        "continuous": "nearest",
        "discrete": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else self._parse_layout(value)

  def _parse_layout(self, obj):
    # Makes the metadata objects of the data layers autocomplete friendly.
    def _parse(obj, ref):
      is_layer = False
      while not is_layer:
        if all([x in obj for x in ["type", "values"]]):
          obj["reference"] = copy.deepcopy(ref)
          if isinstance(obj["values"], list):
            obj["labels"] = {x["label"]:x["id"] for x in obj["values"]}
            obj["descriptions"] = {x["description"]:x["id"] for x in obj["values"]}
          del ref[-1]
          is_layer = True
        else:
          ref.append(k)
          for k, v in obj.items():
            _parse(v, ref)
    for k, v in obj.items():
      ref = [k]
      for k, v in v.items():
        ref.append(k)
        _parse(v, ref)
    return obj

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Solve the reference by obtaining the corresponding metadata object.
    metadata = self.lookup(*reference)
    # Load the data values from the EO data cube.
    data = self._load(metadata, extent)
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data layer '{reference}' does not contain data within the "
        "specified spatio-temporal extent"
      )
    # Format loaded data.
    data = self._format(data, metadata, extent)
    # Mask invalid data.
    data = self._mask(data)
    if data.sq.is_empty:
      warnings.warn(
        f"All values for data layer '{reference}' are invalid within the "
        "specified spatio-temporal extent"
      )
    # Trim the array if requested.
    # This will remove dimension coordinates with only missing or invalid data.
    if self.config["trim"]:
      data = data.sq.trim()
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    return data

  def _load(self, metadata, extent):
    # Check if extent is valid.
    if TIME not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a temporal dimension"
      )
    if X not in extent.dims or Y not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without spatial dimensions"
      )
    # Create a template for the data to be loaded.
    # This is the given extent modified to be correctly understood by ODC:
    # --> Time and space dimensions should have specific names.
    # --> Time dimension values should be in the data cubes native timezone.
    # --> Class should be `xarray.Dataset` instead of `xarray.DataArray`.
    # Note that the CRS does not have to be in the data cubes native CRS.
    # ODC takes care of spatial transformations internally.
    names = {Y: "y", X: "x", TIME: "time"}
    like = extent.sq.tz_convert(self.tz).sq.rename_dims(names).to_dataset()
    # Call ODC load function to load data as xarray dataset.
    data = self.connection.load(
      product = metadata["product"],
      measurements = [metadata["name"]],
      like = like,
      resampling = self.config["resamplers"][metadata["type"]],
      group_by = "solar_day" if self.config["group_by_solar_day"] else None
    )
    # Return as xarray dataarray.
    try:
      data = data[metadata["name"]]
    except KeyError:
      data = xr.DataArray()
    return data

  def _format(self, data, metadata, extent):
    # Step I: Format temporal coordinates.
    # --> Make sure time dimension has the correct name.
    # --> Convert time coordinates back into the original timezone.
    data = data.sq.rename_dims({"time": TIME})
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Step II: Format spatial coordinates.
    # --> Make sure X and Y dims have the correct names.
    # --> Store resolution as an attribute of the spatial coordinate dimensions.
    # --> Add spatial feature indices as a non-dimension coordinate.
    data = data.sq.rename_dims({data.rio.y_dim: Y, data.rio.x_dim: X})
    data = data.sq.write_spatial_resolution(extent.sq.spatial_resolution)
    data.coords["spatial_feats"] = ([Y, X], extent["spatial_feats"].data)
    # Step III: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    data.sq.value_type = self.config["value_type_mapping"][metadata["type"]]
    if isinstance(metadata["values"], list):
      value_labels = {}
      for x in metadata["values"]:
        try:
          label = x["label"]
        except KeyError:
          label = None
        value_labels[x["id"]] = label
      data.sq.value_labels = value_labels
    data[TIME].sq.value_type = "datetime"
    data[Y].sq.value_type = "continuous"
    data[X].sq.value_type = "continuous"
    data["spatial_feats"].sq.value_type = extent["spatial_feats"].sq.value_type
    data["spatial_feats"].sq.value_labels = extent["spatial_feats"].sq.value_labels
    return data

  def _mask(self, data):
    # Step I: Mask nodata values.
    data = masking.mask_invalid_data(data)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded for the bbox of the extent.
    data = data.where(data["spatial_feats"].notnull())
    return data

class GeotiffArchive(Datacube):
  """EO data cube configuration for zipped GeoTIFF files.

  This simple EO data cube configuration assumes each data layer is a GeoTIFF
  file, and that all layers together are bundled in a ZIP archive. It is meant
  for demonstration purposes.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    src : :obj:`str`
      Path to the ZIP archive containing the data layers.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **trim** (:obj:`bool`): Should each retrieved data layer be trimmed?
        Trimming means that dimension coordinates for which all values are
        missing are removed from the array. The spatial dimensions are trimmed
        only at the edges, to maintain their regularity. Defaults to
        :obj:`True`.

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the layout map to the value types used by semantique?
        Defaults to a one-to-one mapping: ::

          {
            "nominal": "nominal",
            "ordinal": "ordinal",
            "binary": "binary",
            "continuous": "continuous",
            "discrete": "discrete"
          }

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {
            "nominal": "nearest",
            "ordinal": "nearest",
            "binary": "nearest",
            "continuous": "nearest",
            "discrete": "nearest"
          }

  """

  def __init__(self, layout = None, src = None, tz = "UTC", **config):
    super(GeotiffArchive, self).__init__(layout)
    self.src = src
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def src(self):
    """:obj:`str`: Path to the ZIP archive containing the data layers."""
    return self._src

  @src.setter
  def src(self, value):
    if value is not None:
      assert os.path.splitext(value)[1] == ".zip"
    self._src = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    if isinstance(value, datetime.tzinfo):
      self._tz = value
    elif isinstance(value, str):
      self._tz = pytz.timezone(value)
    else:
      raise exceptions.UnknownTimeZoneError(
        f"Cannot recognize {value} as a valid timezone specification"
      )

  @property
  def _default_config(self):
    return {
      "trim": True,
      "value_type_mapping": {
        "nominal": "nominal",
        "ordinal": "ordinal",
        "binary": "binary",
        "continuous": "continuous",
        "discrete": "discrete"
      },
      "resamplers": {
        "nominal": "nearest",
        "ordinal": "nearest",
        "binary": "nearest",
        "continuous": "nearest",
        "discrete": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else self._parse_layout(value)

  def _parse_layout(self, obj):
    # Makes the metadata objects of the data layers autocomplete friendly.
    def _parse(obj, ref):
      is_layer = False
      while not is_layer:
        if all([x in obj for x in ["type", "values"]]):
          obj["reference"] = copy.deepcopy(ref)
          if isinstance(obj["values"], list):
            obj["labels"] = {x["label"]:x["id"] for x in obj["values"]}
            obj["descriptions"] = {x["description"]:x["id"] for x in obj["values"]}
          del ref[-1]
          is_layer = True
        else:
          ref.append(k)
          for k, v in obj.items():
            _parse(v, ref)
    for k, v in obj.items():
      ref = [k]
      for k, v in v.items():
        ref.append(k)
        _parse(v, ref)
    return obj

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Solve the reference by obtaining the corresponding metadata object.
    metadata = self.lookup(*reference)
    # Load data.
    # This loads all data in the layer into memory (NOT EFFICIENT!).
    # Only after that is subsets the loaded data in space and time.
    data = self._load(metadata, extent)
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data layer '{reference}' does not contain data within the "
        "specified spatio-temporal extent"
      )
    # Format loaded data.
    data = self._format(data, metadata, extent)
    # Mask invalid data.
    data = self._mask(data)
    if data.sq.is_empty:
      warnings.warn(
        f"All values for data layer '{reference}' are invalid within the "
        "specified spatio-temporal extent"
      )
    # Trim the array if requested.
    # This will remove dimension coordinates with only missing or invalid data.
    if self.config["trim"]:
      data = data.sq.trim()
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    return data

  def _load(self, metadata, extent):
    # Check if extent is valid.
    if TIME not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a temporal dimension"
      )
    if X not in extent.dims and Y not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without spatial dimensions"
      )
    # Load the complete layer.
    data = rioxarray.open_rasterio("zip://" + self.src + "!" + metadata["file"])
    # Prepare the extent for subsetting.
    # This means dimension names need to be aligned to the loaded data.
    # And the time values need to be converted into the loaded data timezone.
    names = {Y: data.rio.y_dim, X: data.rio.x_dim, TIME: "band"}
    like = extent.sq.tz_convert(self.tz).sq.rename_dims(names)
    # Subset temporally.
    bounds = like["band"].values
    times = [np.datetime64(x, "ns") for x in data.attrs["long_name"]]
    keep = [x >= bounds[0] and x <= bounds[1] for x in times]
    data = data.sel({"band": keep})
    data = data.assign_coords({"band": [x for x, y in zip(times, keep) if y]})
    # Subset spatially.
    resampler_name = self.config["resamplers"][metadata["type"]]
    resampler_func = getattr(rasterio.enums.Resampling, resampler_name)
    data = data.rio.reproject_match(like, resampling = resampler_func)
    # Return subsetted data.
    return data

  def _format(self, data, metadata, extent):
    # Step I: Format temporal coordinates.
    # --> Make sure time dimension has the correct name.
    # --> Convert time coordinates back into the original timezone.
    # --> Delete the long_name attribute that stored the band names.
    data = data.sq.rename_dims({"band": TIME})
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    del data.attrs["long_name"]
    # Step II: Format spatial coordinates.
    # --> Make sure X and Y dims have the correct names.
    # --> Store resolution as an attribute of the spatial coordinate dimensions.
    # --> Add spatial feature indices as a non-dimension coordinate.
    data = data.sq.rename_dims({data.rio.y_dim: Y, data.rio.x_dim: X})
    data = data.sq.write_spatial_resolution(extent.sq.spatial_resolution)
    data.coords["spatial_feats"] = ([Y, X], extent["spatial_feats"].data)
    # Step III: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    data.sq.value_type = self.config["value_type_mapping"][metadata["type"]]
    if isinstance(metadata["values"], list):
      value_labels = {}
      for x in metadata["values"]:
        try:
          label = x["label"]
        except KeyError:
          label = None
        value_labels[x["id"]] = label
      data.sq.value_labels = value_labels
    data[TIME].sq.value_type = "datetime"
    data[Y].sq.value_type = "continuous"
    data[X].sq.value_type = "continuous"
    data["spatial_feats"].sq.value_type = extent["spatial_feats"].sq.value_type
    data["spatial_feats"].sq.value_labels = extent["spatial_feats"].sq.value_labels
    # Step V: Give the array a name.
    data.name = os.path.splitext(metadata["file"])[0]
    return data

  def _mask(self, data):
    # Step I: Mask nodata values.
    data = data.where(data != data.rio.nodata)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded for the bbox of the extent.
    data = data.where(data["spatial_feats"].notnull())
    return data
