#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
import app

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys
import networkx as nx


@Configuration()
class nxBfsCommand(StreamingCommand):
    child = Option(
        doc='''
        **Syntax:** **child=***<fieldname>*
        **Description:** Name of the field that holds the child''',
        require=True, validate=validators.Fieldname())

    parent = Option(
        doc='''
        **Syntax:** **parent=***<fieldname>*
        **Description:** Name of the field that holds the parent''',
        require=True, validate=validators.Fieldname())

    bfs_path = Option(
        doc='''
        **Syntax:** **parent=***<fieldname>*
        **Description:** Name of the field that will hold the computed bfs_path''',
        require=False, default="bfs_path", validate=validators.Fieldname())

    bfs_count = Option(
        doc='''
        **Syntax:** **parent=***<fieldname>*
        **Description:** Name of the field that will hold the computed bfs count''',
        require=False, default="bfs_count", validate=validators.Fieldname())

    @Configuration()
    def stream(self, records):
      self.logger.debug('nxbfs: %s', self)
      c = self.child
      p = self.parent
      G = nx.Graph()
      res = []

      for r in records:
        G.add_edge(r[c], r[p])
        bfs=list(nx.bfs_tree(G, r[p])) 
        r[self.bfs_path] = {r[p]: bfs}
        r[self.bfs_count] = len(bfs)
        yield r
        

dispatch(nxBfsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
