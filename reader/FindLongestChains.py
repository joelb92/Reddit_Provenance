import sys
import os
import json
import numpy as np

jsonDir = sys.argv[1]


fileNames = []
chainSizes = []
totalNodeSizes = []
fullchains = []
for file in os.listdir(jsonDir):
    max_chain = 0
    if file.endswith(".json"):
        print('opening '+ file)
        with open(os.path.join(jsonDir,file),'r') as fp:
            graph = json.load(fp)
        links = graph['links']

        current_chain = 0
        max_chain_weblinks = []
        targetToLinkMap = {}
        print("Building link map")
        for link in links:
            # print('t: ',link['target'],' s: ',link['source'])

            targetToLinkMap[link['target']] = link
        print("Searching longest chain in " + str(len(links)) + " links")
        for link in links:
            current_chain = 0
            lasttarget = None
            target = link['target']
            if target == 'dcmnd2v':
                print('')
            nextLink = targetToLinkMap[target]
            source = nextLink['source']
            chain = []
            chain.append(target)
            while source is not None and not lasttarget == target and target in targetToLinkMap:
                nextLink = targetToLinkMap[target]
                source = nextLink['source']
                lasttarget = target
                target = source
                chain.append(source)
                current_chain+=1
            if current_chain > max_chain:
                max_chain = current_chain
                max_chain_weblinks = chain
        fileNames.append(os.path.basename(file))
        chainSizes.append(max_chain)
        fullchains.append(max_chain_weblinks)
        totalNodeSizes.append(len(graph['nodes']))
indexes = np.argsort(np.asarray(totalNodeSizes))[::-1]
filenames_sorted = list(np.asarray(fileNames)[indexes])
chainsizes_sorted = list(np.asarray(chainSizes)[indexes])
fullchains_sorted = list(np.asarray(fullchains)[indexes])
totalNodeSizes_sorted = list(np.asarray(totalNodeSizes)[indexes])
csv = "file,max chain length,totalImages"
html = "<html>\n<title>Chain Lengths</title>\n<body>\n<h2>Graphs sorted by chain size</h2>\n<hr>\n<ul>\n"

for i in range(0,len(filenames_sorted)):
    csv += "\n"+filenames_sorted[i]+","+str(chainsizes_sorted[i])+','+str(totalNodeSizes_sorted[i])#+","+(",".join(fullchains_sorted[i]))
    html += "\n<li><a href=\"" + filenames_sorted[i].split('.')[0]+'.html'+"\">"+str(chainsizes_sorted[i])+": "+filenames_sorted[i]+" - Total nodes: "+str(totalNodeSizes_sorted[i])+"</a>"
html+="\n</ul><hr></body></html>"
with open(os.path.join("",'chainsizes.txt'),'w') as fp:
    fp.write(csv)

with open(os.path.join("", 'chainsizes.html'), 'w') as fp:
    fp.write(html)
