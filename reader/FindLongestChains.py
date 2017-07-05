import sys
import os
import json
import numpy as np

jsonDir = sys.argv[1]


fileNames = []
chainSizes = []
fullchains = []
for file in os.listdir(jsonDir):
    if file.endswith(".json"):
        with open(os.path.join(jsonDir,file),'r') as fp:
            graph = json.load(fp)
        links = graph['links']
        max_chain = 0
        current_chain = 0
        max_chain_weblinks = []
        targetToLinkMap = {}
        for link in links:
            targetToLinkMap[link['target']] = link
        for link in links:
            target = link['target']
            nextLink = targetToLinkMap[target]
            source = nextLink['source']
            chain = []
            while source is not None and source in link and not source == 'root':
                nextLink = targetToLinkMap[target]
                source = nextLink['source']
                chain.append(source)
                current_chain+=1
            if current_chain > max_chain:
                max_chain = current_chain
                max_chain_weblinks = chain
        fileNames.append(os.path.basename(file))
        chainSizes.append(max_chain)
        fullchains.append(max_chain_weblinks)
indexes = np.argsort(np.asarray(chainSizes))
filenames_sorted = list(np.asarray(fileNames)[indexes])
chainsizes_sorted = list(np.asarray(chainSizes)[indexes])
fullchains_sorted = list(np.asarray(fullchains)[indexes])

csv = "file,max chain length"
for i in range(0,len(filenames_sorted)):
    csv += "\n"+filenames_sorted[i]+","+chainsizes_sorted[i]+","+",".join(fullchains_sorted[i])
with open(os.path.join("",'chainsizes.txt'),'wb') as fp:
    fp.write(csv)
