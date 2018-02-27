#! /usr/bin/python

'''
This is a crawler for RSS feeds.
Note that MD5 is used to digest URLs, which is vulnerable
to collision!

Created on Dec 17, 2012
@version: 1.0 on Dec. 18th, 2012

@author: Mingjie Qian
'''

import sys
import MySQLdb
from bs4 import BeautifulSoup
import bs4
import os
import urllib2
from datetime import datetime, tzinfo, timedelta
from dateutil import parser
import hashlib
import HTMLParser

class EST(tzinfo):
    def utcoffset(self,dt):
        return timedelta(hours=5,minutes=30)
    def tzname(self,dt):
        return "EST"
    def dst(self,dt):
        return timedelta(0)

class RSSFeedCrawler:
    
    def __init__(self, DBUser, DBPass, DBCleanUp, crawl_conf_path, data_dir_path):
        self.DBUser = DBUser
        self.DBPass = DBPass
        self.DBCleanUp = DBCleanUp
        self.crawl_conf_path = crawl_conf_path
        self.data_dir_path = data_dir_path
        self.RSSFeedMap = {}
        self.DBConn = None
        self.desDateFormat = "%Y%m%d%H%M%S%Z"
        self.est = EST()
        
    def buildRSSFeedMap(self):
        
        fd = open(self.crawl_conf_path)
        soup = BeautifulSoup(fd, "html.parser")
        fd.close()
        channels = soup.select('feeds language[name=english] > channel')
        self.RSSFeedMap = {}
        for channel in channels:
            channelName = channel['name']
            channelMap = {}
            channelMap['URL'] = channel.url.get_text()
            channelMap['XPath'] = channel.xpath.get_text()
            if channel.img_xpath is not None:
                channelMap['img_xpath'] = channel.img_xpath.get_text()
            else:
                channelMap['img_xpath'] = None
        
            self.RSSFeedMap[channelName] = channelMap
            
    def configureDatabase(self):
        
        try:
            self.DBConn = MySQLdb.connect("localhost", user = "root", passwd = "1234")
            print "Database connection established."
        except:
            pass
        
        cursor = self.DBConn.cursor()
        DBName = 'RSSFeedCrawler'
        if self.DBCleanUp:
            cursor.execute('DROP DATABASE IF EXISTS %s;' % DBName)
        cursor.execute('CREATE DATABASE IF NOT EXISTS %s;' % (DBName,))
        cursor.execute('USE %s;' % DBName)
        for channelName in self.RSSFeedMap.iterkeys():
            cursor.execute(
                    "CREATE TABLE IF NOT EXISTS %s (" % channelName
                    + "id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, "
                    + "goodPage boolean not null default 1, "
                    + "md5 CHAR(32) NOT NULL, "
                    + "PRIMARY KEY (id), "
                    + "KEY md5_idx (md5));"
                    )
    
    def crawl(self):
        for channelName in self.RSSFeedMap.iterkeys():
            self.crawlChannel(channelName)
        pass
    
    def crawlChannel(self, channelName):
        
        channelDir = self.data_dir_path + os.path.sep + channelName
        if not os.path.exists(channelDir):
            os.makedirs(channelDir)
        
        sql = "SELECT COUNT(*) FROM %s WHERE goodPage = 1;" % channelName
        cursor = self.DBConn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        docID = int(result[0])
        
        channelURL = self.RSSFeedMap[channelName]['URL']
        RSSDOM = BeautifulSoup(urllib2.urlopen(channelURL), "html.parser")
        items = RSSDOM.select("item")
        
        # Crawl items in the RSS feed channel
        for item in items:
            docID = self.crawlItem(item, channelName, channelDir, docID)
            
    def crawlItem(self, item, channelName, channelDir, docID):
        
        channelMap = self.RSSFeedMap[channelName]
        title = item.title.get_text().strip()
        description = item.description.get_text().strip()
        pubDate = item.pubdate.get_text().strip()
        dt = parser.parse(pubDate)
        newdate = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo = self.est)
        titlePubDate = newdate.strftime(self.desDateFormat)
        linkURL = item.link.get_text()
        if not linkURL:
            return docID
        
        if "http" not in linkURL:
            linkURL = channelMap['URL'] + linkURL
        
        m = hashlib.md5()
        m.update(linkURL)
        md5 = m.hexdigest()
        
        cursor = self.DBConn.cursor()
        sql = "SELECT id, goodPage FROM %s WHERE md5 = '%s';" % (channelName, md5)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            if result[1]:
                quality = "good"
            else:
                quality = "bad"
            print "Find an already-processed %s link: %s" % (quality, linkURL)
            return docID
        
        # Crawl the web page specified by linkURL
        print "Find a new link: " + linkURL
        print "Crawling... " + linkURL
        
        HTMLContent = self.crawlURL(linkURL)
        
        if not HTMLContent:
            return docID
        
        try:
            docTree = BeautifulSoup(HTMLContent, 'html5lib')
        except HTMLParser.HTMLParseError as e:
            sys.stderr.write(e)
            return docID
        else:
            docID += 1
        
        fileSimpleName = "%s-%08d-%s" % (channelName, docID, titlePubDate)
        filePathPrefix = channelDir + os.path.sep + fileSimpleName
        if_success = self.saveHTMLContent(filePathPrefix, docID, title, description, pubDate, linkURL, docTree, channelName)
        if not if_success:
            docID -= 1
        sql = "INSERT INTO %s (md5, goodPage) VALUES('%s', %d)" % (channelName, md5, if_success)
        cursor.execute(sql)
        # Commit your changes in the database
        self.DBConn.commit()
        
        return docID
            
    def crawlURL(self, linkURL):
        try:
            file_handle = urllib2.urlopen(linkURL)
            return file_handle.read()
        except:
            print 'Cannot open the URL: %s' %  linkURL
            return ''
    
    def saveHTMLContent(self, filePathPrefix, docID, title, description, pubDate, linkURL, docTree, channelName):
        
        textXPath = self.RSSFeedMap.get(channelName).get("XPath")
        imgXPath = self.RSSFeedMap.get(channelName).get("img_xpath")
        
        contentFilePath = filePathPrefix + '.txt'
        fd = open(contentFilePath, 'w')
        
        fd.write("<DOC>\n")
        fd.write("<DOCNO>%d</DOCNO>\n" % docID)
        fd.write("<URL>%s</URL>\n" % linkURL)
        fd.write("<TITLE>%s</TITLE>\n" % title)
        fd.write("<TIME>%s</TIME>\n" % pubDate)
        fd.write("<ABSTRACT>\n")
        fd.write(description)
        fd.write("</ABSTRACT>\n")
        fd.write("<TEXT>\n")
        
        elements = docTree.select(textXPath)
        if elements and len(elements) > 0:
            parent = elements[0].parent
            for child in parent.children:
                content = ''
                if type(child) == bs4.element.NavigableString:
                    content = child.strip()
                elif type(child) == bs4.element.Tag and child.name == 'p':
                    content = child.get_text()
                else:
                    continue
                content = content.replace('\n', ' ')
                if content:
                    fd.write(content)
                    fd.write('\n')
            fd.write("</TEXT>\n")
            fd.write("</DOC>\n")
            fd.close()
        else:
            # print >> sys.stderr, 'Empty content!'
            fd.close()
            sys.stderr.write('Empty content!\n')
            os.remove(contentFilePath)
            return False
        
        HTMLFilePath = filePathPrefix + '.html'
        fd = open(HTMLFilePath, "w")
        fd.write(str(docTree))
        fd.close()

        if not imgXPath:
            return True

        imgTags = docTree.select(imgXPath)
        if not imgTags:
            return True
        
        imgTag = imgTags[0]
        if not imgTag:
            return True
        imgLink = imgTag['src']
        if not imgLink:
            return True
        imgLink = imgLink.replace(" ", "%20")
        
        if 'cnn' in channelName and (not imgLink or imgLink.endswith('.gif')):
            imgLinkFound = False
            imgXPath = imgXPath[:imgXPath.rfind(' img[src]')] + ' script'
            elements = docTree.select(imgXPath)
            for element in elements:
                if type(element) == bs4.element.Tag:
                    script = element.text
                    startIdx = script.rfind('http')
                    endIdx = script.rfind('.jpg')
                    if startIdx != -1 and endIdx != -1:
                        imgLink = script[startIdx:endIdx + 4]
                        imgLinkFound = True
                        break
            
            if not imgLinkFound:
                return True
        
        extension = imgLink[imgLink.rindex("."):]
        if extension.find('?') != -1:
            extension = extension[0:extension.find('?')]
        
        if extension == '.gif':
            return True
        
        if extension.lower() == '.video':
            extension = '.jpg'
        
        imgFilePath = filePathPrefix + extension
        req = urllib2.Request(imgLink)
        response = urllib2.urlopen(req)
        image = response.read()
        fd = open(imgFilePath, 'wb')
        fd.write(image)
        fd.close()
        
        return True

    def disconnectDatabase(self):
        self.DBConn.close ()
        print "Database connection terminated."
    
def main(DBUser, DBPass, DBCleanUp, crawl_conf_path, data_dir_path):
    """
    Crawl the RSS feed sites listed in an XML file located in crawl_conf_path.
    Images and text contents are stored in a directory specified in data_dir_path.
    """
    crawler = RSSFeedCrawler(DBUser, DBPass, DBCleanUp, crawl_conf_path, data_dir_path)
    
    crawler.buildRSSFeedMap()
    crawler.configureDatabase()
    crawler.crawl()
    crawler.disconnectDatabase()
    
    print 'Mission complete!'

if __name__ == '__main__':
    
    if (len(sys.argv) < 6):
        sys.stdout.write('usage: python RSSFeedCrawler.py <DBUser> <DBPass> <DBCleanUp> <crawl_conf_path> <data_dir_path>') 
        sys.exit(1)

    DBUser = sys.argv[1]
    DBPass = sys.argv[2]
    DBCleanUp = sys.argv[3].lower() == 'true'
    crawl_conf_path = sys.argv[4]
    data_dir_path = sys.argv[5]
    
    main(DBUser, DBPass, DBCleanUp, crawl_conf_path, data_dir_path)
