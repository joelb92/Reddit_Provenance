#!/usr/bin/env python
import pymysql
import urllib2
import json
import collections
import types
import time
import datetime
import sys
import os
from pprint import pprint
import re
import urlparse
import csv
import traceback
import json


# ----------------------
# Functions 
# ----------------------

# Show script usage
def showUsage():
    print
    print "Usage:"
    print
    print "- To read all new threads from a subreddit:"
    print "python reader.py /r/yoursubreddithere"
    print
    print "- To get all the comments from the threads read"
    print "python reader.py --get-comments"
    print
    sys.exit()

# Read threads
def readThreads(subreddit, cur):
    newThreads = 0
    existingThreads = 0
    # print "found " + str
    for t in subreddit:

        # Get the thread info
        threadId = t['data']['id']
        title = t['data']['title']
        permalink = t['data']['permalink']
        link = t['data']['url']
        score = t['data']['score']
        created = t['data']['created_utc']
        print "Found thread: " + title

        # Save it to the database. Duplicate threads will be ignored due to the UNIQUE KEY constraint
        try:
            cur.execute("""INSERT INTO threads (id_thread, id_sub, title, url, link, score, created) values (%s, 1, %s, %s, %s, %s, %s)""", (threadId, title, permalink,link, int(score), created))
            newThreads += 1
            print "New thread: " + title
        except pymysql.err.IntegrityError as e:
            existingThreads += 1

    # Print a summary
    print "Got " + str(newThreads + existingThreads) + " threads."
    print "Inserted " + str(newThreads) + " new threads"
    print "Found " + str(existingThreads) + " already existing threads"

    # Log totals
    global totalNewThreads
    totalNewThreads += newThreads
    global totalExistingThreads
    totalExistingThreads += existingThreads
# Recursive function to read comments
def readComments(obj, threadId, threadUrl, cur,parentID):
    newComments = 0
    existingComments = 0
    for i in obj:

        # Basic info, present both in Title and Comment
        commentId = i['data']['id']
        content = ""
        url = ""
        score = 0
        created = 0
        if 'created_utc' in i['data']:
            created = i['data']['created_utc']
        else:
            print "*** WARNING: created_utc not found in this record -> " + commentId

        # Is it a comment?
        if 'body' in i['data']:

            url = threadUrl + commentId
            commentID = commentId
            content = i['data']['body']
            links = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', content)
            imgLinks = []
            for link in links:
                if ".jpg" in link or ".png" in link or ".gif" in link or "i.imgur" in link:
                    imgLinks.append(link)
                elif "imgur" in link:
                    imgName = link.split("/")[-1]
                    newLink = "http://i.imgur.com/" + imgName + ".png"
                    imgLinks.append(newLink)
            if len(imgLinks) > 0:
                print "found links: " + str(imgLinks)
                commentImages[commentID] = imgLinks
            if parentID == "":
                parentID = "root"
            commentIDtoParentMap[commentID] = parentID
            childList = []
            if parentID in commentIDtoChildMap:
                childList = commentIDtoChildMap[parentID]
                childList.append(commentID)
            else:
                childList = []
                childList.append(commentID)
            commentIDtoChildMap[parentID] = childList
            ups = int(i['data']['ups'])
            downs = int(i['data']['downs'])
            score = ups - downs

        # Or is it the title post?
        elif 'selftext' in i['data']:
            commentID = commentId
            url = i['data']['url']
            content = i['data']['selftext']
            score = i['data']['score']

            links = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', content)
            imgLinks = []
            for link in links:
                if ".jpg" in link or ".png" in link or ".gif" in link or "i.imgur" in link:
                    imgLinks.append(link)
                elif "imgur" in link:
                    imgName = link.split("/")[-1]
                    newLink = "http://i.imgur.com/" + imgName + ".png"
                    imgLinks.append(newLink)
            if len(imgLinks) > 0:
                print
                "found links: " + str(imgLinks)
                commentImages[commentID] = imgLinks
            commentIDtoParentMap[commentID] = "root"
            childList = []
            if "root" in commentIDtoChildMap:
                childList = commentIDtoChildMap["root"]
                childList.append(commentID)
            else:
                childList = []
                childList.append(commentID)
            commentIDtoChildMap[parentID] = childList

        # Save!
        # try:
        # 	cur.execute("""INSERT INTO comments (id_comment, id_thread, comment, url, score, created) values (%s, %s, %s, %s, %s, %s)""", (commentId, threadId, content, url, int(score), created))
        # 	newComments += 1
        # except pymysql.err.IntegrityError as e:
        # 	existingComments += 1

        # Does it have a reply?
        if 'replies' in i['data'] and len(i['data']['replies']) > 0:
            readComments(i['data']['replies']['data']['children'], threadId, threadUrl, cur,commentId)

    # Print a Summary
    print "Inserted " + str(newComments) + " new comments"
    print "Found " + str(existingComments) + " already existing comments"

    # Log totals
    global totalNewComments
    totalNewComments += newComments
    global totalExistingComments
    totalExistingComments += existingComments

def getJsonPageForPostCount(pcount,name):
    return ".json?sort=top&t=all&count=" + str(pcount) + "&after=" + name


def downloadImage(image_url,out_dir,out_name):
    print "Scanning URLs..."
    image_urls = []
    image_urls.append(image_url)
    # Create an output directory
    try:
        os.makedirs(out_dir)
    except:
        pass #print "Error creating directory",out_dir

    i = 0
    for each in image_urls:
        each = each.replace(")", "")
        try:
            print "Reading URL:",each
            f = urllib2.urlopen(each,timeout=10)
            data = f.read()
            f.close()

            print "   Datasize:",len(data)

            bn = urlparse.urlsplit(each).path.split('/')[-1]
            imgName = out_name
            out_path = os.path.join(out_dir,imgName)
            f = open(out_path,'wb')
            f.write(data)
            f.close()

            # query_csv.writerow([each,each,out_path,len(data)])
            i+=1


        except KeyboardInterrupt:
            raise

        except:
            print "Error Fetching URL:",each
            traceback.print_exc()

def downloadImages(image_urls,out_dir,k):
    print "Scanning URLs..."

    # Create an output directory
    try:
        os.makedirs(out_dir)
    except:
        pass #print "Error creating directory",out_dir

    query_csv = csv.writer(open(os.path.join(out_dir,'query_results.csv'),'wb'))
    query_csv.writerow(["title","url","path","size"])
    imageNameList = {}
    i = 0
    for each in image_urls:
        each0 = each[0].replace(")", "")
        try:
            print "Reading URL:",each0
            f = urllib2.urlopen(each0,timeout=10)
            data = f.read()
            f.close()
            print "   Datasize:",len(data)
            bn = urlparse.urlsplit(each0).path.split('/')[-1]
            ftype = bn.split(".")
            ftype = ftype[len(ftype) - 1]
            imgName = "g"+"%03d_"%k + each[1] + "."+ftype
            out_path = os.path.join(out_dir,imgName)
            imageNameList[each[1]]=(imgName,out_path,each)
            f = open(out_path,'wb')
            f.write(data)
            f.close()
            i+=1


        except KeyboardInterrupt:
            raise

        except:
            print "Error Fetching URL:",each
            imageNameList[each[1]] = (None,None,None)
            traceback.print_exc()
    return imageNameList


def requestJson(url, delay):
    while True:
        try:
            # Reddit API Rules: "Make no more than thirty requests per minute"
            if delay < 2:
                delay = 2
            time.sleep(delay)

            req = urllib2.Request(url, headers=hdr)
            response = urllib2.urlopen(req)
            jsonFile = response.read()
            print "Requested URLS..."
            return json.loads(jsonFile)
        except Exception as e:
            print e

# ----------------------
# Script begins here
# ----------------------

# Setup ------------------------------------------

# Url, header and request delay
# If we don't set an unique User Agent, Reddit will limit our requests per hour and eventually block them


userAgent = "unix:com.notredame.mediforScraper:v0.2 (by /u/aDutchofMuch)"
if userAgent == "":
    print
    print "Error: you need to set an User Agent inside this script"
    print
    sys.exit()
hdr = {'User-Agent' : userAgent}
baseUrl = "http://www.reddit.com"

# Read args
shouldReadComments = False
shouldReadThreads = False
postCount = 0
maxPosts = 1000
if len(sys.argv) >= 2:
    if sys.argv[1] == "--get-comments":
        shouldReadThreads = True
        delay = 2
        print "Reading comments"
    else:
        subreddit = sys.argv[1]
        subredditUrl = "" #baseUrl + subreddit + "/top/" + getJsonPageForPostCount(postCount,"")
        shouldReadComments = True
        delay = 30
        print "Reading threads from " + subredditUrl
else:
    showUsage()

print "Starting crawler"
print "Press ctrl+c to stop"
print

# Database connection
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='Jbrogan92c', db='reddit', charset='utf8')
cur = conn.cursor()

# Start! -----------------------------------------
totalNewThreads = 1
nextID = ""
global commentImages
global commentIDtoParentMap
global commentIDtoChildMap
commentImages = {}
commentIDtoParentMap = {}
commentIDtoChildMap = {}
outputDirectory = "./default/"
if len(sys.argv) >= 2:
    if sys.argv[1] == "--get-comments":
        outputDirectory =  sys.argv[2] #"/Users/joel/Downloads/simple-reddit-crawler-master/reader/RedditDataset2"
singleDir = True
while (not shouldReadThreads and postCount < maxPosts) or shouldReadThreads or not shouldReadComments:

    # Log starting time
    startingTime = datetime.datetime.now()
    # Totals to log in the database
    totalNewThreads = 0
    totalExistingThreads = 0
    totalNewComments = 0
    totalExistingComments = 0

    # Read threads
    if not shouldReadThreads:

        # Read the Threads
        print "Requesting new threads..."

        if postCount == 0:
            subredditUrl = baseUrl + subreddit + "/top.json?sort=top&t=all&"
        else:
            getReq = getJsonPageForPostCount(postCount,nextID)
            subredditUrl = baseUrl + subreddit + "/top" + getReq
        postCount += 25
        print "reading url: " + subredditUrl
        jsonObj = requestJson(subredditUrl, delay)

        # Save the threads
        nextID = jsonObj['data']['after']
        print "after = " + nextID
        readThreads(jsonObj['data']['children'], cur)
        conn.commit()

    # Read comments
    if not shouldReadComments:

        # Get all the threads urls
        cur.execute("SELECT * FROM threads")
        threads = dict()
        for row in cur.fetchall():
            threads[row[0]] = (row[3],row[4],row[5])

        # Read them all!
        for k, w in threads.iteritems():
            v = w[1]
            threadTitle = w[0]
            title_nice = threadTitle.replace(" ", "_")
            title_nice = title_nice.replace("/","_")
            title_nice = title_nice.replace("/n","_")

            rootImageLink = w[2]
            # Prepare the http request
            print
            print "Requesting thread comments..."
            jsonData = requestJson(baseUrl + urllib2.quote(v.encode('utf8')) + ".json", delay)

            #Download the root image
            if singleDir:
                outdir = outputDirectory
            else:
                outdir = os.path.join(outputDirectory, str(k))
            if ".jpg" in rootImageLink or ".png" in rootImageLink or ".gif" in rootImageLink or "i.imgur" in rootImageLink:
                rootImageLink = rootImageLink
            elif "imgur" in rootImageLink:
                imgName = rootImageLink.split("/")[-1]
                newLink = "http://i.imgur.com/" + imgName + ".png"
                rootImageLink = newLink
            suffix = rootImageLink.split(".")
            suffix = suffix[len(suffix)-1]
            imname = "g" + "%03d_" % k + "i" + "%03d_" % 0 + "." + suffix
            downloadImage(rootImageLink,outdir,imname)
            commentImages['0000'] = rootImageLink
            commentIDtoParentMap['0000'] = 'root'
            commentIDtoParentMap['root'] = ['0000']
            # Read the Thread
            # 0 = title
            postData = jsonData[0]['data']['children']
            readComments(postData, k, v, cur,"")

            # 1 = comments
            data = jsonData[1]['data']['children']
            readComments(data, k, v, cur,"")

            # Save!
            # conn.commit()

            #Build JSON file for provenance graph
            outputCSV = ""
            allImageLinks = []
            nodes = []
            links = []
            for commentID in commentImages.keys():
                imageLinks = commentImages[commentID]
                for l in imageLinks:
                    allImageLinks.append((l,commentID))
            imageNamesList = downloadImages(allImageLinks, outdir,k)
            for commentID in commentImages.keys():
                imageWebLinks = commentImages[commentID]
                node = {}
                node['nodeConfidenceScore'] = 1.0
                node['id'] = commentID
                imageNamesData = imageNamesList[commentID]
                node['file']=imageNamesData[1]
                node['fileid']=imageNamesData[0]
                node['URL'] = imageNamesData[1]
                nodes.append(node)
            for commentID in commentImages.keys():
                currentParent = commentIDtoParentMap[commentID]
                # Hop up comment tree until we find a comment with an image
                while currentParent not in commentImages.keys() and not currentParent == "root":
                    currentParent = commentIDtoParentMap[currentParent]
                link = {}
                link['source'] = currentParent
                link['target'] = commentID
                link['relationshipConfidenceScore'] = 1.0
                links.append(link)

            count = 0
            finalDict = {}
            finalDict['nodes'] = nodes
            finalDict['links'] = links
            finalDict['directed'] = True
            if not os.path.exists(outputDirectory):
                os.makedirs(outputDirectory)
            with open(os.path.join(outputDirectory,"graph_"+str(k)+".json"),'w') as f:
                json.dump(finalDict,f)

            # for commentID in commentIDtoParentMap.keys():
            #     outputCSV += str(commentID) + "," + str(commentIDtoParentMap[commentID])
            #     imageLinks = commentImages[commentID]
            #     for l in imageLinks:
            #         parts = l.split("/")
            #         bn = imageNamesList[count]
            #         if len(parts) > 0:
            #             name = parts[-1]
            #         outputCSV += "," + bn
            #         count +=1
            #     outputCSV += "\n"
            # # f = open(os.path.join(outputDirectory,str(k),"treeData.csv"), 'w')
            # # f.write(outputCSV)
            # # f.close()
            commentImages = {}
            commentIDtoParentMap = {}
            commentIDtoChildMap = {}
    # Finishing time
    endingTime = datetime.datetime.now()

    # Log this run in the database
    print
    print "Finishing up. Logging this run..."
    if shouldReadComments:
        print "Total new threads: " + str(totalNewThreads)
        print "Total existing threads (skipped, not inserted): " + str(totalExistingThreads)
    if shouldReadThreads:
        print "Total new comments: " + str(totalNewComments)
        print "Total existing comments (skipped, not inserted): " + str(totalExistingComments)
    print "---------------------------------------------------"
    print
    cur.execute("""INSERT INTO logs (startingTime, endingTime, newThreads, ignoredThreads, newComments, ignoredComments) values (%s, %s, %s, %s, %s, %s)""", (startingTime, endingTime, totalNewThreads, totalExistingThreads, totalNewComments, totalExistingComments))
    conn.commit()

# Close the connection
conn.commit()
cur.close()
conn.close()

