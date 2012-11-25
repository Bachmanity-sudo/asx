from pandas import *
from pandas.util.testing import set_trace as st

import numpy as np
from bs4 import BeautifulSoup
import mechanize
import os,sys
from datetime import date, datetime, time, timedelta
import logging
import logging.handlers

asx_path = '/home/humed/python/ASXdata/'
os.chdir(asx_path)

formatter = logging.Formatter('|%(asctime)-6s|%(message)s|','%Y-%m-%d %H:%M')
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(formatter)
logging.getLogger('').addHandler(consoleLogger)

fileLogger = logging.handlers.RotatingFileHandler(filename=asx_path + 'asx_grabber.log',maxBytes = 1024*1024, backupCount = 9)
fileLogger.setLevel(logging.ERROR)
fileLogger.setFormatter(formatter)
logging.getLogger('').addHandler(fileLogger)

logger = logging.getLogger('ASX')
logger.setLevel(logging.INFO)

class asx_grabber():
   
    def __init__(self):
        self.asx_path = '/home/humed/python/ASXdata/'
        self.url_head = 'http://www.sfe.com.au/Content/reports/'
        self.url_file_head = 'EODWebMarketSummary'
        self.trunk = self.asx_path + 'htm/'
        self.asx_dirs = {'futures/day':'ZFD','futures/night':'ZFN','futures/total':'ZFT','options/day':'ZOD','options/night':'ZON','options/total':'ZOT'}
        self.date = datetime.date(datetime.today() - timedelta(days=1)) #this is the date of the download, ie, yesterday's data
        self.br = mechanize.Browser() # Browser
        self.br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1) # Follows refresh 0 but not hangs on refresh > 0
        self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')] # User-Agent (this is cheating, ok?)

    def get_asx(self,date,dirs,tails): #Get the data and save to the directory structure outlined in self.trunk + self.asx_dirs
        os.chdir(self.trunk + dirs) #get into the correct subdirectory

        if date.weekday() < 5: #if Monday to Friday
           url_file_date = str(self.date.year)[2:] + "%02d" % (self.date.month) + "%02d" % (self.date.day) #YYMMDD format
           url_file = self.url_file_head + url_file_date + tails + '.htm' #create the filename: ie, EODWebMarketSummary120906ZOT.htm
           url = self.url_head + url_file #add the url head
           try: 
               r = self.br.open(url) #open sfe website
               f = open(url_file,'wb') #write the htm file to disk
               f.write(r.read()) #write to disk
               f.close()
               info_text = 'Download of %s to %s successful' % (url,self.trunk + dirs)
               logger.info(info_text)
           except mechanize.HTTPError, error_text:
               logger.error(error_text)


    def get_asx_dailys(self):
        for dirs,tails in self.asx_dirs.iteritems():
            self.get_asx(self.date,dirs,tails)    

if __name__ == '__main__':
   asx_grab = asx_grabber()
   asx_grab.get_asx_dailys()
