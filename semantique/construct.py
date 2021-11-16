__all__ = [
  "concept",
  "entity",
  "event",
  "resource",
  "appearance",
  "artifacts",
  "atmosphere",
  "reflectance",
  "topography",
  "result",
  "self",
  "collection",
  "category",
  "time_instant",
  "time_interval"
]

from semantique.extent import SpatialExtent, TemporalExtent

class CubeProxy(dict):

  def __init__(self, obj):
    super(CubeProxy, self).__init__(obj)

  def _append_verb(self, name, collector = False, **kwargs):
    verb = {"type": "verb", "name": name, "params": kwargs}
    if "do" in self:
      self["do"].append(verb)
      return CubeCollectionProxy(self) if collector else self
    else:
      new = {"type": "processing_chain", "with": self, "do": [verb]}
      return CubeCollectionProxy(new) if collector else CubeProxy(new)

  def evaluate(self, operator, y = None, **kwargs):
    if y is None:
      kwargs.update({"operator": operator})
    else:
      kwargs.update({"operator": operator, "y": y})
    return self._append_verb("evaluate", **kwargs)

  def extract(self, dimension, component = None, **kwargs):
    if component is None:
      kwargs.update({"dimension": dimension})
    else:
      kwargs.update({"dimension": dimension, "component": component})
    return self._append_verb("extract", **kwargs)

  def filter(self, filterer, **kwargs):
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def groupby(self, grouper, **kwargs):
    kwargs.update({"grouper": grouper})
    return self._append_verb("groupby", collector = True, **kwargs)

  def label(self, label, **kwargs):
    kwargs.update({"label": label})
    return self._append_verb("label", **kwargs)

  def reduce(self, dimension, reducer, **kwargs):
    kwargs.update({"dimension": dimension, "reducer": reducer})
    return self._append_verb("reduce", **kwargs)

  def replace(self, y, **kwargs):
    kwargs.update({"y": y})
    return self._append_verb("replace", **kwargs)

class CubeCollectionProxy(dict):

  def __init__(self, obj):
    super(CubeCollectionProxy, self).__init__(obj)

  def _append_verb(self, name, combiner = False, **kwargs):
    verb = {"type": "verb", "name": name, "params": kwargs}
    if "do" in self:
      self["do"].append(verb)
      return CubeProxy(self) if combiner else self
    else:
      new = {"type": "processing_chain", "with": self, "do": [verb]}
      return CubeProxy(new) if combiner else CubeCollectionProxy(new)

  def compose(self, **kwargs):
    return self._append_verb("compose", combiner = True)

  def concatenate(self, dimension, **kwargs):
    kwargs.update({"dimension": dimension})
    return self._append_verb("concatenate", combiner = True, **kwargs)

  def merge(self, reducer, **kwargs):
    kwargs.update({"reducer": reducer})
    return self._append_verb("merge", combiner = True, **kwargs)

  def evaluate(self, operator, y = None, **kwargs):
    if y is None:
      kwargs.update({"operator": operator})
    else:
      kwargs.update({"operator": operator, "y": y})
    return self._append_verb("evaluate", **kwargs)

  def extract(self, dimension, component = None, **kwargs):
    if component is None:
      kwargs.update({"dimension": dimension})
    else:
      kwargs.update({"dimension": dimension, "component": component})
    return self._append_verb("extract", **kwargs)

  def filter(self, filterer, **kwargs):
    kwargs.update({"filterer": filterer})
    return self._append_verb("filter", **kwargs)

  def groupby(self, grouper, **kwargs):
    kwargs.update({"grouper": grouper})
    return self._append_verb("groupby", **kwargs)

  def label(self, label, **kwargs):
    kwargs.update({"label": label})
    return self._append_verb("label", **kwargs)

  def reduce(self, dimension, reducer, **kwargs):
    kwargs.update({"dimension": dimension, "reducer": reducer})
    return self._append_verb("reduce", **kwargs)

  def replace(self, y, **kwargs):
    kwargs.update({"y": y})
    return self._append_verb("replace", **kwargs)

def concept(*reference):
  obj = {"type": "concept", "reference": reference}
  return CubeProxy(obj)

def entity(*reference, property = None):
  obj = {"type": "concept", "reference": ("entity",) + reference}
  if property is not None:
    obj["property"] = property
  return CubeProxy(obj)

def event(*reference, property = None):
  obj = {"type": "concept", "reference": ("event",) + reference}
  if property is not None:
    obj["property"] = property
  return CubeProxy(obj)

def resource(*reference):
  obj = {"type": "resource", "reference": reference}
  return CubeProxy(obj)

def appearance(*reference):
  obj = {"type": "resource", "reference": ("appearance",) + reference}
  return CubeProxy(obj)

def artifacts(*reference):
  obj = {"type": "resource", "reference": ("artifacts",) + reference}
  return CubeProxy(obj)

def atmosphere(*reference):
  obj = {"type": "resource", "reference": ("atmosphere",) + reference}
  return CubeProxy(obj)

def reflectance(*reference):
  obj = {"type": "resource", "reference": ("reflectance",) + reference}
  return CubeProxy(obj)

def topography(*reference):
  obj = {"type": "resource", "reference": ("topography",) + reference}
  return CubeProxy(obj)

def result(name):
  obj = {"type": "result", "name": name}
  return CubeProxy(obj)

def self():
  obj = {"type": "self"}
  return CubeProxy(obj)

def collection(*cubes):
  obj = {"type": "collection", "elements": list(cubes)}
  return CubeCollectionProxy(obj)

def category(label):
  obj = {"type": "category", "label": label}
  return obj

def geometry(value, **kwargs):
  obj = {"type": "geometry", "value": SpatialExtent(value, **kwargs)}

def time_instant(value, **kwargs):
  obj = {"type": "time_instant", "value": TemporalExtent(value, **kwargs)}
  return obj

def time_interval(*bounds, **kwargs):
  obj = {"type": "time_interval", "value": TemporalExtent(*bounds, **kwargs)}
  return obj