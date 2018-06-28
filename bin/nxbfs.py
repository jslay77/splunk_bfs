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
      child = self.child
      parent = self.parent
      graph = nx.Graph()

      for item in records:
        graph.add_edge(item[child], item[parent])
        bfs_path = list(nx.bfs_tree(graph, item[parent])) 
        bfs_path_str = ' '.join(str(i) for i in bfs_path)
        item[self.bfs_path] = bfs_path_str
        item[self.bfs_count]=len(bfs_path)
        yield item
        

dispatch(nxBfsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
