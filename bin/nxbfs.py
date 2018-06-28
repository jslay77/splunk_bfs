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
      c = self.child
      p = self.parent
      G = nx.Graph()
      res = []

      for r in records:
        G.add_edge(r[c], r[p])
        bfs=list(nx.bfs_tree(G, r[p])) 
        res.append({r[p]: bfs})
      yield { 'result': res }

    def reduce(self, records):
      res = []
      for r in records:
        for num in range(len(r['result'])):
          res.append(r['result'][num])
      yield { 'result': res }

dispatch(nxBfsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
