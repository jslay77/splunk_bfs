#!/usr/bin/env python

from __future__ import absolute_import, division, print_function, unicode_literals
import app

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys

from collections import deque
import sets

@Configuration()
class BfsCommand(StreamingCommand):
	""" Runs a Breadth-First Search using two input fields

	##Syntax

	.. code-block::
		bfs parent=<field> child=<field>

	##Description:

	Follows a parent-child relationship through an entire eventset and produces a MV bfs_path field with a list of nodes along with a bfs_count field.

	##Example

	..code-block::
		index = main |fields foo bar| bfs parent=foo child=bar

	This example finds all relationship paths between :code:`foo` and :code:`bar` in the
	:code:`main index`.

	"""
	parent = Option(
		doc='''
		**Syntax:** **parent=***<fieldname>*
		**Description:** Name of the parent field''',
		require=True, validate=validators.Fieldname())

	child = Option(
		doc='''
		**Syntax:** **child=***<fieldname>*
		**Description:** Name of the child field''',
		require=True, validate=validators.Fieldname())

	bfs_count = Option(
		doc='''
		**Syntax:** **bfs_count=***<fieldname>*
		**Description:** Name of the resulting bfs_count field''',
		require=False, validate=validators.Fieldname(), default="bfs_count")

	bfs_path = Option(
		doc='''
		**Syntax:** **bfs_path=***<fieldname>*
		**Description:** Name of the resulting bfs_path field''',
		require=False, validate=validators.Fieldname(), default="bfs_path")


	def stream(self, records):
		preds = {}

		def bfs(root):
			b_path = []
		
			Q = deque()
			V = set()
			V.add(root)
			Q.append(root)
			while Q:
				t = Q.popleft()
				b_path.append(t)
				if t in preds:
					for e in preds[t]:
						if e not in V:
							V.add(e)
							Q.append(e)
			return b_path

		self.logger.debug('BfsCommand.stream: %s', self)
		for record in records:
			tid = record[self.parent].replace(' ','')
			if record[self.child] == '':
				preds[tid] = []
			else:
				preds[tid] = record[self.child].replace(' ','').split(',')
			try:
				bfs_path = bfs(tid)
				record[self.bfs_path] = bfs_path
				record[self.bfs_count] = len(bfs_path)
			except Exception, e:
				self.logger.error('Exception: %s', e)
			yield record

dispatch(BfsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
