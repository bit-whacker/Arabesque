import sys

fname = "../data/citeseer.graph"
infile = open(fname)
vertex_idx = {}

for line in infile:
	line = line.rstrip().split(" ")
	if len(line) < 2: raise Exception("Vertex id or label not present")
	vertex_idx[int(line[0])] = int(line[1])

infile.close()

infile = open(fname)
graph_info = {}

edges_position = 0

for line in infile:
	line = line.rstrip().split(" ")
	if len(line) <= 2: continue
	edges = []
	for i in xrange(2, len(line)):
		vertex = int(line[i])
		edges.append((vertex,vertex_idx[vertex],edges_position))
		edges_position += 1
	graph_info[int(line[0])] = edges

print edges_position
infile.close()
