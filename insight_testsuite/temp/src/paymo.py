'''
Created on Nov 5, 2016

@author: marjan sadat alavi
'''

import os
import sys
import logging
from _collections import defaultdict
import time

log = logging.getLogger("paymo")
logging.basicConfig(level=logging.DEBUG,format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
#----------------------------------------------------------------------------

class Graph:
    def __init__(self,adjList):
        self.graph = defaultdict(set)
        self.nodes = set()
        self.addEdges(adjList)
    def addEdges(self,adjList):
        """Builds the graph by adding the adjacency list read from the input file."""
        for u,v in adjList:
            self.graph[u].add(v)
            self.graph[v].add(u)
            self.nodes.add(v)
            self.nodes.add(u)
    def getAdjacents(self,u):
        """Returns the set of adjacent nodes to user u."""
        if u in self.graph:
            return self.graph[u] 
    def getEdges(self):
        """Returns the adjacency set for all the nodes within the graph."""
        return self.graph 
    def getNodes(self):
        """Returns all the nodes within the graph."""
        return self.nodes     
    def isAdjacent(self,u,v):
        """Checks whether input nodes u and v are adjacent."""
        if(not u in self.graph or not v in self.graph):
            return False
        if u in self.graph[v]: #or v in self.graph[u]  (Its a symmetric rule for undirected graphs)
            return True
        return False        
    def degree_2_away(self,u,v):
        """Checks whether input nodes u and v are friends or friends of friends."""
        if(not u in self.graph or not v in self.graph):
            return False
        if self.isAdjacent(u,v):
            return True
        uEdges = self.graph[u]
        vEdges = self.graph[v]
        intersection = uEdges & vEdges #equivalent to uEdges.intersection(v)
        if len(intersection):
            return True
        return False     
    def degree_4_away(self,u,v):
        """Checks whether input nodes u and v fall within 4-degree friendship of each other.
        To speed up the function, the code traverses the graph in a bidirectional fashion: starting from u 
        and v both at the same time, and performing set intersection when required. 
        """
        if(not u in self.graph or not v in self.graph):
            return False
        if self.isAdjacent(u,v):
            return True
        uEdges = self.graph[u]
        vEdges = self.graph[v]
        intersection = uEdges & vEdges
        if len(intersection):
            return True       
        for w in vEdges:
            wEdges = self.graph[w]   
            if wEdges is None:
                continue
            intersection = uEdges & wEdges
            if len(intersection):
                return True            
            for x in uEdges:
                xEdges = self.graph[x]
                if xEdges is None:
                    continue
                intersection = wEdges & xEdges
                if len(intersection):
                    return True               
        return False            
#----------------------------------------------------------------------------       
         
def readFromFile(inpFile):
    """Reading input from the input file to get transaction endpoints."""
    adjacencyList=[]
    if(not os.path.isfile(inpFile)):
        log.error('The file %s does not exist!',inpFile)
        sys.exit(1)  
    with open(inpFile) as fp:
        next(fp)
        for line in fp:
            curline = line.split(",")
            if len(curline)>=5:
                adjacencyList.append((curline[1].strip(),curline[2].strip()))
    return adjacencyList
#----------------------------------------------------------------------------

if __name__=="__main__":
    currentPath = os.path.dirname(__file__)
    parentPath = (os.path.dirname(currentPath))    
    batchInpFile = parentPath+'/paymo_input/batch_payment.txt'
    streamInpFile = parentPath+'/paymo_input/stream_payment.txt'
    outFileF1 = parentPath+'/paymo_output/output1.txt'
    outFileF2 = parentPath+'/paymo_output/output2.txt'
    outFileF3 = parentPath+'/paymo_output/output3.txt'

    f1 = open(outFileF1,'w+')
    f2 = open(outFileF2,'w+')
    f3 = open(outFileF3,'w+')
    
    startTime = time.clock()
    adjList = readFromFile(batchInpFile)
    endTime =time.clock()
    log.debug('Time (in seconds) taken to read the batch input file: %s',endTime-startTime)
    
    startTime = time.clock()
    g = Graph(adjList)
    endTime =time.clock()
    log.debug('Time (in seconds) taken to build the initial graph: %s',endTime-startTime)
    
    startTime = time.clock()
    streamAdjList = readFromFile(streamInpFile) 
    endTime =time.clock()
    log.debug('Time (in seconds) taken to read the streaming input file: %s',endTime-startTime)

    startTime = time.clock()
    for u,v in streamAdjList:
        if g.isAdjacent(u,v):
            f1.write("trusted\n")
        else:
            f1.write("unverified\n")
        if g.degree_2_away(u,v):
            f2.write("trusted\n")
        else:
            f2.write("unverified\n")   
        if g.degree_4_away(u,v):
            f3.write("trusted\n") 
        else:
            f3.write("unverified\n")
    endTime =time.clock()
    log.debug('Time (in seconds) taken to run all three features altogether on the whole streaming file: %s',endTime-startTime)
    
    '''  
    startTime = time.clock()
    for u,v in streamAdjList:
        if g.isAdjacent(u,v):
            f1.write("trusted\n")
        else:
            f1.write("unverified\n")
    endTime =time.clock()
    log.debug('Time (in seconds) taken to run isAdjacent function on the whole streaming file: %s',endTime-startTime)

    startTime = time.clock()
    for u,v in streamAdjList:
        if g.degree_2_away(u,v):
            f2.write("trusted\n")
        else:
            f2.write("unverified\n")   
    endTime =time.clock()
    log.debug('Time (in seconds) taken to run degree_2_away function on the whole streaming file: %s',endTime-startTime)
        
    startTime = time.clock()
    for u,v in streamAdjList:
        if g.degree_4_away(u,v):
            f3.write("trusted\n") 
        else:
            f3.write("unverified\n")
    endTime =time.clock()
    log.debug('Time (in seconds) taken to run degree_4_away function on the whole streaming file: %s',endTime-startTime)
    '''
            
    f1.close()
    f2.close()
    f3.close()
    print('done')