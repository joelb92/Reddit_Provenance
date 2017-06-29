import sys
import os
import json
import numpy as np

jsonDir = sys.argv[1]


fileNames = []
chainSizes = []
for file in os.listdir(jsonDir):
    with open(file,'r') as fp:
        graph = json.load(fp)
    links = graph['links']
    max_chain = 0
    current_chain = 0
    for link in links:
        source = link['source']
        target = link['target']
        chain = []
        while source is not None and not source == "root" and not source == "0000":
            chain.append(source)
            source = link[source]
            current_chain+=1

        if current_chain > max_chain:
            max_chain = current_chain
    fileNames.append(os.path.basename(file))
    chainSizes.append(max_chain)
indexes = np.argsort(np.asarray(chainSizes))
filenames_sorted = fileNames[indexes]
chainsizes_sorted = chainSizes[indexes]

csv = "file,max chain length"
for i in range(0,len(filenames_sorted)):
    csv += "\n"+filenames_sorted[i]+","+chainsizes_sorted[i]
with open(os.path.join(jsonDir,'chainsizes.txt'),'wb') as fp:
    fp.write(csv)
