#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
import app

from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import sys

@Configuration(requires_preop=True)
class Bfs2Command(ReportingCommand):
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
      graph_temp = { 'children': [], 'parents': [] }
      graph = []

      for record in records:
       for item in record['children']:
        graph_temp['children'].append(item)
       for item in record['parents']:
        graph_temp['parents'].append(item)
  
      for num in range(len(graph_temp['children'])):
       graph.append([ graph_temp['children'][num], graph_temp['parents'][num] ])

      
      yield { 'array': graph }

dispatch(Bfs2Command, sys.argv, sys.stdin, sys.stdout, __name__)
