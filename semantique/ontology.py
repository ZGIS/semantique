from abc import abstractmethod

from semantique import exceptions
from semantique.processor.core import QueryProcessor
from semantique.processor.structures import CubeCollection

class Ontology(dict):

  def __init__(self, rules = None):
    obj = {} if rules is None else rules
    super(Ontology, self).__init__(obj)
    self._rules = obj

  @property
  def rules(self):
    return self._rules

  @rules.setter
  def rules(self, value):
    self._rules = value

  def lookup(self, *reference):
    obj = self
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.InvalidReferenceError(
          f"Ontology does not contain concept '{reference}'"
        )
    return obj

  @abstractmethod
  def translate(self, *reference, property = None, extent, factbase,
                eval_obj = None, **config):
    pass

class Semantique(Ontology):

  def __init__(self, rules = None):
    super(Semantique, self).__init__(rules)

  def translate(self, *reference, property = None, extent, factbase,
                eval_obj = None, **config):
    ruleset = self.lookup(*reference)
    processor = QueryProcessor({}, factbase, self, extent, **config)
    if eval_obj is not None:
      processor._set_eval_obj(eval_obj)
    if property is None:
      properties = [processor.call_handler(ruleset[obj]) for obj in ruleset]
      if len(properties) == 1:
        out = properties[0]
      else:
        out = CubeCollection(properties).merge("all")
    else:
      try:
        property = ruleset[property]
      except KeyError:
        raise exceptions.InvalidReferenceError(
          f"Property '{property}' is not defined for concept '{reference}'"
        )
      out = processor.call_handler(property)
    out.name = reference[-1]
    return out