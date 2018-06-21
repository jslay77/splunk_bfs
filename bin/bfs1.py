# | inputlookup stepmaster | join type=left [search index=stepmaster] | fields task*
# | inputlookup stepmaster | join type=left [search index=stepmaster] | fields task* | bfs deps=task_pred id=task_id

import sys
import splunk.Intersplunk
import string
import logging
import tempfile
from collections import deque
import sets

logfile = logging.StreamHandler(open(tempfile.gettempdir() + "/bfs.log", "a"))
logfile.setLevel(logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logfile)

results = splunk.Intersplunk.readResults(None, None, True)

preds = {}
task_status = {}

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

def process_res():
	for result in results:
		tid = result[id].replace(' ','')
		if result[deps] == '':
			preds[tid] = []
		else:
			preds[tid] = result[deps].replace(' ','').split(',')
		try:
			result['bfs_path'] = bfs(tid)
			result['bfs_count'] = len(result['bfs_path'])
		except Exception, e:
			logger.exception(e)
		
def main():
	try:
		process_res()
	except Exception, e:
		logger.exception(e)

try:
	if __name__ == '__main__':
		logger.debug('start')

		keywords, argvals = splunk.Intersplunk.getKeywordsAndOptions()

		deps = argvals.get('deps')
		id = argvals.get('id')
	
		main()
except Exception, e:
	logger.exception(e)

splunk.Intersplunk.outputResults(results)
