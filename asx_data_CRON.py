'''
ASX web scrapper - automatic monitoring of ASX futures market 

Copyright (C) 2013 David Hume, Electricty Authority, New Zealand.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

This is used to scrape the ASX data htm table into its separate sub tables.
The subtables, since 14/7/2009 included the Benmore and Otahuhu AXS futures prices
A two letter code is used to split the htm table into in consituent sub-tables - python is pretty cool doing this, but I'm sure there are probably better ways too...
Ok, the sub-tables:
BB: 90 Day Bank Bill (100 minus yield % p.a)
TY: 3 Year Stock (100 minus yield % p.a)
TN: 10 Year Stock (100 minus yield % p.a)
ZO: NZ 30 Day OCR Interbank (RBNZ Interbank Overnight Cash)
EA: NZ Electricity Futures (Otahuhu) (NZ Electricity Futures (Otahuhu))
EE: NZ Electricity Futures (Benmore) (NZ Electricity Futures (Benmore))
EB: NZ Electricity Strip Futures (Otahuhu) (NZ Electricity Strip Futures (Otahuhu))
EF: NZ Electricity Strip Futures (Benmore) (NZ Electricity Strip Futures (Benmore))

Run everyday at 5minutes past 9am with the following crontab entry:
5 9 * * * /usr/bin/python /home/dave/python/asx/asx_data_CRON.py >> /home/dave/python/asx/asx_grabber_CRON.log 2>&1
'''
#Lets import modules - thankyou for open source software...
from pandas import *
from pandas.util.testing import set_trace as st

import numpy as np
import matplotlib.mlab as mp
from bs4 import BeautifulSoup
import mechanize
import os,sys
from datetime import date, datetime, time, timedelta
import time
import logging
import logging.handlers
import calendar
import StringIO

#Initial setup
asx_path = '/home/dave/python/asx/'
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

#Setup the class
class asx_grabber():
   
    def __init__(self):
        self.asx_path = '/home/dave/python/asx/'
        self.asx_path_P = '/run/user/dave/gvfs/smb-share:server=ecomfp01,share=common/ASX_daily/'
        #self.asx_path_P = '/home/dave/.gvfs/common on ecomfp01/ASX_daily/'
        self.url_head = 'http://www.sfe.com.au/Content/reports/'
        self.url_file_head = 'EODWebMarketSummary'
        self.asx_warehouse_file = 'asx_futures.h5'
        self.trunk = self.asx_path + 'htm/'
        self.asx_dirs = {'futures/total':'ZFT'} #{'futures/day':'ZFD','futures/night':'ZFN','futures/total':'ZFT'} #,'options/day':'ZOD','options/night':'ZON','options/total':'ZOT'}
        self.date = datetime.date(datetime.today() - timedelta(days=1)) #this is the date of the download, ie, yesterday's data
        self.br = mechanize.Browser() # Browser
        self.br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1) # Follows refresh 0 but not hangs on refresh > 0
        self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')] # User-Agent (this is cheating, ok?)
        self.months={'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

        self.asx_htm_data = None
        self.allasxdata = None
        self.last_date = None
        self.got_htm_file = False
        
        
    def get_last_save_date(self): #Return the date of the last htm save 
        x=[]
        if len(os.listdir(self.trunk + self.asx_dirs.keys()[0])) > 0: #if there exist any files at all
            for filename in os.listdir(self.trunk + self.asx_dirs.keys()[0]): #/home/humed/python/ASXdata/htm/futures/total/EODWebMarketSummary120910ZFT.htm
                if 'Summary' in filename:
                    date_str = filename.split('.')[0].split('Summary')[1][0:6]
                    x.append(date(int('20' + date_str[0:2]),int(date_str[2:4]),int(date_str[4:6])))
            self.last_date = np.sort(x)[-1]
            print 'Last ASX data file saved on ' + str(self.last_date)
        else: #ok, no files exist. Lets set the date to (one day before) the start of the ASX futures market, which happens to be....dum-de-da...
            self.last_date = date(2009,7,13)
        
    def get_asx(self,dirs,tails): #Get the data and save to the directory structure outlined in self.trunk + self.asx_dirs
        os.chdir(self.trunk + dirs) #get into the correct subdirectory
        url_file_date = str(self.last_date.year)[2:] + "%02d" % (self.last_date.month) + "%02d" % (self.last_date.day) #YYMMDD format
        url_file = self.url_file_head + url_file_date + tails + '.htm' #create the filename: ie, EODWebMarketSummary120906ZOT.htm
        url = self.url_head + url_file #add the url head
        try: 
            r = self.br.open(url) #open sfe website
            self.asx_htm_data = r.read()
            asx_to_disk = open(url_file,'wb') #write the htm file to disk
            asx_to_disk.write(self.asx_htm_data) #write to disk
            asx_to_disk.close()  #close file
            #print 'HTM data: ' + self.asx_htm_data
            info_text = 'Download of %s to %s successful' % (url,self.trunk + dirs)
            logger.info(info_text)
            url_file_size = os.path.getsize(url_file)
            if url_file_size > 5000:
               self.got_htm_file = True
            else: 
               self.got_htm_file = False 
        except mechanize.HTTPError, error_text:
            logger.error(error_text)
            self.got_htm_file = False

    def get_asx_dailys(self): #download and save all ASX files for the date required 
        self.get_last_save_date()

        while self.last_date < self.date:
            tic = time.clock()
            self.last_date = self.last_date + timedelta(days=1) #next day from last save
            if self.last_date.weekday() < 5: #if Monday to Friday
                #print 'Getting data for %s' % str(self.last_date)
                for dirs,tails in self.asx_dirs.iteritems():
                    self.get_asx(dirs,tails) #download the htm files to directory structure
                    if self.got_htm_file == True:
                       self.get_asx_table()     #returns self.allasxdata, a dictionary of dataframes in the ASX web data 
                       self.update_warehouse(tails)
            toc = time.clock()    
            info_text = 'Data for %s processed in %s seconds' % (self.last_date,str(toc-tic))
            logger.info(info_text)

    def update_warehouse(self,tails):
        '''Ok, this works but is extremely inefficient in terms of the amount of drive space required for the panel object - basically its embarassing.  Something is not quite here - probably a user error...'''
        if self.last_date == date(2009,7,14): #Create ASX HDF5 data file for the first time
           asx_futures = HDFStore(self.asx_path + self.asx_warehouse_file) #open the asx data warehouse!
           ota = Panel({self.last_date:self.allasxdata['EA']})
           ben = Panel({self.last_date:self.allasxdata['EE']})
           asx_futures['OTA_' + tails] = ota
           asx_futures['BEN_' + tails] = ben
           asx_futures.close()      
        else:            
           tic=time.clock()
           asx_futures = HDFStore(self.asx_path + self.asx_warehouse_file) #open the asx data warehouse!
           ota = asx_futures['OTA_' + tails]     #get ota
           ben = asx_futures['BEN_' + tails]
           asx_futures.close()   
           os.remove(self.asx_path + self.asx_warehouse_file)
           toc=time.clock()
           info_text = 'Opening ' + self.asx_warehouse_file + ' took %s seconds' % (str(toc-tic))
           logger.info(info_text)
           tic=time.clock()
           ota = ota.join(Panel({self.last_date:self.allasxdata['EA']}),how='outer') #join the new data to the exisiting panel data - this took a bit of figuring out. Outer is the union of the indexes so when a new row appears for a new quater, Nulls or NaNs full the remainder of the dataframe
           ben = ben.join(Panel({self.last_date:self.allasxdata['EE']}),how='outer')
           toc=time.clock()
           info_text = 'Data join took %s seconds' % (str(toc-tic))
           logger.info(info_text)
           tic=time.clock()
           asx_futures = HDFStore(self.asx_path + self.asx_warehouse_file) #open the asx data warehouse!
           asx_futures['OTA_' + tails] = ota      #overwrite ota_xxx
           asx_futures['BEN_' + tails] = ben 
           asx_futures.close() #closing ASX warehouse
           toc=time.clock()
           info_text = 'Resaving ' + self.asx_warehouse_file + ' took %s seconds' % (str(toc-tic))
           logger.info(info_text)
           to_excel = True
           if to_excel == True:
               #Spit to XLS ****THIS IS SLOW**** comment out if updating h5 file
               try:      #to local linux box
                  ota.to_excel(self.asx_path + 'OTA_' + tails + '.xls') #spit to excel
                  ben.to_excel(self.asx_path + 'BEN_' + tails + '.xls')
               except error_text:
                 logger.error(error_text)
   
               try:      #to P:/ASX_dailys/
                  ota.to_excel(self.asx_path_P + 'OTA_' + tails + '.xls') #spit to excel
                  ben.to_excel(self.asx_path_P + 'BEN_' + tails + '.xls')
               except error_text:
                  logger.error(error_text)

 
            
    def get_asx_table(self):
        soup = BeautifulSoup(self.asx_htm_data)
        body = soup.html.body
        tables = body.findAll('table')
        self.scrape_asx_table(tables[1]) #there are 3 tables the middle one has the data, hence the [1], this returns a dictionary of all asx data in dataframes (not a panel, yet...)


    def scrape_asx_table(self,asx_table): #Scrape the table in the htm file.  This consists of 8 subtables tagged with BB/EA/EE etc where EA and EE are Otahuhu and Benmore
        rows = asx_table.findAll('tr') #using beautifulsoup to get the rows of the table
        all_data = []
        for row in rows:  #pass the rows
            #entries_titles = row.findAll('td', { "align": "left", "colspan":10 })  #you could get sub table titles this way
            entries = row.findAll('td')  #get a list of table data
            row_data = []
            for entry in entries:
                etext = entry.text.replace('-', '')
                etext = etext.replace(',','')
                row_data.append(etext) #append data and replace '-' with '', and replace ',' with ''
            all_data.append(row_data)
        #Filter the data in the table to get rid on empty rows and rows that say Click here...etc..
        colnames = filter(lambda d: d[0]=='Expiry',all_data) #get the column names
        colnames = colnames[0] #assume they are all the same...colnames = ['Expiry','Open','High','Low','Last','Sett','Sett Chg','Op Int','Op Int Chg','Volume']
        ddd = filter(lambda d: d[0]!='' and d[0]!='Expiry' and d[0]!='Click here to view settlement price and volume graph',all_data) #filter out repetitive rows
    
        df_names=[] #get list of dataframes from table
        for d in ddd:
            if len(d) < 2:
                df_names.append(d[0][:2])
    
        xx  = list(mp.find(map(lambda d: d[0][:2] in df_names ,ddd))) #Ok, get position of the sub-table headers - this is the first two letters, BB/TY/TN/ZO/EA/EE/EB/EF - make it a list
        xx.append(len(ddd)) #append the total length of the filtered table data

        splits = {}  #We need to split the table data into the 8 separate tables, there may be a better way to do this - but for now do it like this...
        ii=0         #set a counter
        for split in np.arange(len(xx)-1): #loop through the number of sub-tables (in this case there are eight separate tables to be passed...
            splits[ddd[xx[ii]][0][:2]] = ddd[(xx[ii]+1):xx[ii+1]] #add to the dictionary the correct rows of data as indexed by xx - this is a bit yuck.
            ii+=1
        self.allasxdata = {} #create an empty dictionary which will consist of dataframes and will be panelified
        for df_name,data in splits.iteritems():
            converted = {}
            for name, col in zip(colnames, zip(*data)):
                #print name
                #print col
                try:
                    converted[name] = np.array(col, dtype=float)
                except:
                    converted[name] = np.array(col)
            #print converted
            df_temp = DataFrame(converted, columns=colnames)
            df_temp = df_temp.replace(u'','nan') #replace empty values with nans
            df_temp.index = df_temp['Expiry']  
            del df_temp['Expiry']
            for c in df_temp.columns:  #now float all values and make sparse
                df_temp[c] = df_temp[c].map(lambda x: float(x))
            df_temp = df_temp.to_sparse()
            #rint df_temp

            df_temp.index = df_temp.index.map(lambda x: datetime.date(datetime(int(x[-4:]),self.months[x[:3]],calendar.monthrange(int(x[-4:]),self.months[x[:3]])[1]))) #convert index to datetime.date with last day of the month
            

            self.allasxdata[df_name] = df_temp

    def get_table_by_id(tables):
        table_dict = {}
    
        for table in tables:
    
            name = _get_attr(table, 'id')
            if name:
                table_dict[name] = table
    
        return table_dict

            
if __name__ == '__main__':
   asx_grab = asx_grabber()
   axsdata = asx_grab.get_asx_dailys()            
 
