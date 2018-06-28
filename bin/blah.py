#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
import app
import json

from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import sys
import networkx as nx


@Configuration()
class blahCommand(ReportingCommand):
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
    def map(self, records):
      self.logger.debug('blah[map]: %s', self)
      child = self.child
      parent = self.parent
      graph = nx.Graph()
      for item in records:
        graph.add_edge(item[child], item[parent])
      yield { 'json_data': nx.node_link_data(graph) }

    def reduce(self, records):
      blah = []
      for item in records:
        graph_j = json.loads(item['json_data'])
        blah.append(graph_j)
      yield { 'foo': blah }
      
dispatch(blahCommand, sys.argv, sys.stdin, sys.stdout, __name__)
