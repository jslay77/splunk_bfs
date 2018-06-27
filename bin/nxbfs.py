#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
import app

from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import sys
import networkx as nx


@Configuration(requires_preop=True)
class nxBfsCommand(ReportingCommand):
    child = Option(
        doc='''
        **Syntax:** **child=***<fieldname>*
        **Description:** Name of the field that will hold the computed sum''',
        require=True, validate=validators.Fieldname())

    parent = Option(
        doc='''
        **Syntax:** **parent=***<fieldname>*
        **Description:** Name of the field that will hold the computed sum''',
        require=True, validate=validators.Fieldname())

    @Configuration()
    def map(self, records):
      child_field = self.child
      parent_field = self.parent

      children = []
      parents = []

      for record in records:
        children.append(record[child_field])
        parents.append(record[parent_field])
      yield { 'children': children, 'parents': parents }

    def reduce(self, records):
      G = nx.Graph()
      res = []
      for r in records:
        for num in range(len(r['children'])):
          G.add_edge(r['children'][num], r['parents'][num])
        
        for num in range(len(r['parents'])):
          bfs=list(set(sum(list(nx.algorithms.bfs_tree(G, r['parents'][num]).edges()), ())))
          res.append({r['parents'][num]: bfs})
      yield { 'result': res }

dispatch(nxBfsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
