import unittest
import  os
import paymo
from paymo import Graph
import collections
from _collections import defaultdict

currentPath = os.path.dirname(__file__)
parentPath = (os.path.dirname(currentPath))    
batchInpFile = parentPath+'/paymo_input/batch_payment.txt'
streamInpFile = parentPath+'/paymo_input/stream_payment.txt'
#----------------------------------------------------------------------------

adjList = []
graph = defaultdict(set)


class TestPaymo(unittest.TestCase):
    def setUp(self):
        testName = self.shortDescription()
        if testName == "Testing graph functions":
            adjList.append((49466,6989))
            adjList.append((6989,8552))
            adjList.append((8552,2562))
            adjList.append((2562,13167))
            adjList.append((13167,6289))
            adjList.append((6289,146))
            adjList.append((6289,199))
        
            graph[49466]={6989}
            graph[6989]={49466,8552}
            graph[8552]={6989,2562}
            graph[2562]={8552,13167}
            graph[13167]={2562,6289}
            graph[6289]={13167,146,199}
            graph[146]={6289}
            graph[199]={6289}
          
    def tearDown(self):
        testName = self.shortDescription()
        if testName == "Testing graph functions":
            adjList=[]
            graph.clear()            

    def testFile(self):
        """Testing input file format"""
        def testEachFile(inputFile):
            self.assertTrue(os.path.isfile(inputFile))
            self.firstLine="time, id1, id2, amount, message"
            with open(inputFile, "r") as fp:
                firstLine = fp.readline().strip()
                self.assertEqual(firstLine,self.firstLine)
                for line in fp:
                    self.assertGreaterEqual(len(line.split(",")),5)
                    self.assertNotEqual(line[1].strip(),line[2].strip())
                    self.assertIsInstance(line[1].strip(),str)
                    self.assertIsInstance(line[2].strip(),str)                 
        [testEachFile(inp) for inp in[batchInpFile,streamInpFile]]

    def testGraph(self):
        """Testing graph functions"""
        self.graph = Graph(adjList)
        self.assertTrue(self.graph)
        self.nodes = self.graph.getNodes()
        self.edges = self.graph.getEdges()
        self.assertTrue(self.nodes)
        self.assertTrue(self.edges)
        for node in self.nodes:
            self.assertSetEqual(graph[node],self.graph.getAdjacents(node))  
        self.assertDictEqual(graph,self.graph.getEdges())
        self.assertFalse(self.graph.isAdjacent(49466, 2562))
        self.assertTrue(self.graph.isAdjacent(13167,6289))
        self.assertFalse(self.graph.degree_2_away(13167, 49466))
        self.assertTrue(self.graph.degree_2_away(8552, 49466))
        self.assertFalse(self.graph.degree_4_away(49466, 67)) 
        self.assertTrue(self.graph.degree_4_away(13167, 49466))
#----------------------------------------------------------------------------         

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPaymo)
    unittest.TextTestRunner(verbosity=2).run(suite)