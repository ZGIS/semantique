from semantique.processor.core import QueryProcessor

class QueryRecipe(dict):

  def __init__(self, results = None):
    obj = {} if results is None else results
    super(QueryRecipe, self).__init__(obj)

  def execute(self, factbase, ontology, space, time, **config):
    qp = QueryProcessor.parse(self, factbase, ontology, space, time, **config)
    return qp.optimize().execute()