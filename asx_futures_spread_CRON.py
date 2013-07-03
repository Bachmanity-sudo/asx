#Script to monitor trading activity on the ASX NZ Electricity futures at Benmore and Otahuhu
#
#Run with the following Cron script, i.e, every 10 minutes between the hours of 9 and 5pm (actual trading occurs between 10:30am and 4pm
#*/10 9-17 * * * /usr/bin/python /home/dave/python/ASXdata/asx_futures_spread_CRON.py >> /home/dave/python/ASXdata/asx_futures_spread_CRON.log 2>&1


from pandas import *
from pandas.util.testing import set_trace as st

import numpy as np
from bs4 import BeautifulSoup
import mechanize
import os,sys
from datetime import date, datetime, time, timedelta
import logging
import logging.handlers
import calendar
import warnings
warnings.filterwarnings('ignore',category=pandas.io.pytables.PerformanceWarning)

#Initial setup
asx_path = '/home/dave/python/ASX_data/'
os.chdir(asx_path)

formatter = logging.Formatter('|%(asctime)-6s|%(message)s|','%Y-%m-%d %H:%M')
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(formatter)
logging.getLogger('').addHandler(consoleLogger)

fileLogger = logging.handlers.RotatingFileHandler(filename=asx_path + 'asx_spreads.log',maxBytes = 1024*1024, backupCount = 9)
fileLogger.setLevel(logging.ERROR)
fileLogger.setFormatter(formatter)
logging.getLogger('').addHandler(fileLogger)

logger = logging.getLogger('ASX')
logger.setLevel(logging.INFO)

#Setup the class
class asx_spreads_grabber():
   
    def __init__(self):
        self.asx_path = '/home/dave/python/ASX_data/'
        #self.asx_path_P = '/home/dave/.gvfs/common on ecomfp01/ASX_daily/'
        self.asx_path_P = '/run/user/dave/gvfs/smb-share:server=ecomfp01,share=common/ASX_daily/'
        self.sites = {'Otahuhu':'http://www.sfe.com.au/content/prices/rtp15ZFEA.html','Benmore':'http://www.sfe.com.au/content/prices/rtp15ZFEE.html'} #,'http://www.asx.com.au/sfe/daily_monthly_reports.htm'}
        self.warehouse_filename = {'Benmore':self.asx_path + 'futures_spread_ben.h5','Otahuhu':self.asx_path + 'futures_spread_ota.h5'}
        self.br = mechanize.Browser() # Browser
        self.br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1) # Follows refresh 0 but not hangs on refresh > 0
        self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')] # User-Agent (this is cheating, ok?)
        self.months={'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
        self.last_check = {'Otahuhu':'ota_last_check.csv','Benmore':'ben_last_check.csv'}
        self.asx_futures={}
        self.time_now = datetime.now()
        self.time_now_str =  "%s-%s-%s %s:%s" % (str(self.time_now.year), str(self.time_now.month), str(self.time_now.day), str(self.time_now.hour), str(self.time_now.minute))
        self.last_datetime = datetime(2012,11,1,1,1)

        
    def read_last(self,csvfile):
        try:
            lf= open(csvfile,'r')
            fs=''
            for l in lf:
                fs += l
            lf.close()
            return fs
        except:
            logger.info('First run, creating dummy last reads')
            fs = ''
        return fs

    def string_convert(self,df):
        import StringIO
        output = StringIO.StringIO()
        df.to_csv(output)
        output.seek(0)
        string = output.read()
        return string
    
    
    def get_table_by_id(self,tables):
        table_dict = {}

        for table in tables:
            name = self._get_attr(table, 'id')
            if name:
                table_dict[name] = table

        return table_dict

    def _get_attr(self,elt, needle):
        elsetab_id = 1
        for kind, value in elt.attrs.iteritems():
        
            if kind == needle:
                return value
            else:
                return 'table_' + str(elsetab_id)  #give the table an id table_1 etc,
                elsetab_id += 1
        return None

    def scrape_data(self):
        for sitename,site in self.sites.iteritems():
            logger.info('Opening ' + sitename + ' data from ' + site)
            r = self.br.open(site) #open sfe website
            htmltext = r.read() #read website html
            self.asx_futures[sitename] = self.get_asx_table(htmltext)

    def type_changer(self,df):
        '''Function to change data types suitable for hdf5 storage'''
        def type_change(x):    
            def value_change(x):
                if type(x) is unicode:
                    if x != '':
                        return float(x)
                    if x == '':
                        return np.NaN
                elif type(x) == datetime:
                    return str(x)
                else:
                    return x
            return x.map(lambda x: value_change(x))

        def utf8_ascii(x):
            udata=x.decode("utf-8")
            return udata.encode("ascii","ignore")
		
        df = df.apply(type_change)
        df['Last Trade DateTime'] = df['Last Trade DateTime'].map(lambda x: str(x).replace('nan','')) 
        df = df.rename(columns = dict(zip(df.columns,df.columns.map(lambda x: utf8_ascii(x)))),index = dict(zip(df.index,df.index.map(lambda x: utf8_ascii(x)))))
        return df
        
    def get_asx_table(self,htmltext):
        soup = BeautifulSoup(htmltext)
        body = soup.html.body
        tables = body.findAll('table')
        tables_by_id = self.get_table_by_id(tables)
        asx_table = tables_by_id['table_1']
        frame = self.scrape_asx_table(asx_table)
        frame = self.type_changer(frame)
        return frame

    def scrape_asx_table(self,asx_table):
        rows = asx_table.findAll('tr')
        colnames = ['Expiry','Bid','Ask','Open','High','Low','Last Trade','Last Trade Date','Last Trade Time','Change','Traded Volume','Previous Settlement']

        all_data = []
        for row in rows:
            entries = row.findAll('td')
            row_data = []
            for entry in entries:
                if self._get_attr(entry, 'class') == 'td_spacer':
                    continue
                row_data.append(entry.text.replace(',','').replace(u'\xa0',''))
            all_data.append(row_data)
    
        converted = {}
        for name, col in zip(colnames, zip(*all_data)):
            try:
                converted[name] = np.array(col, dtype=float)
            except:
                converted[name] = np.array(col)
        df = DataFrame(converted, columns=colnames)
        df.index = df['Expiry']
        df = df[1:]
        del df['Expiry']
    
        #Return date time for the last trade and add to df
        last_trade_datetime = []
        for datex,timex in zip(df['Last Trade Date'],df['Last Trade Time']):
            if datex != u'':   #if we have a date
                if timex != u'': #and a time, then get datetime obj
                    last_trade_datetime.append(datetime(int('20' + datex.split('/')[2]),int(datex.split('/')[1]),int(datex.split('/')[0]),int(timex.split(':')[0]),int(timex.split(':')[1])))
            else:
                last_trade_datetime.append('')
        last_trade_datetime = Series(last_trade_datetime,index = df.index)
        df['Last Trade DateTime'] = last_trade_datetime
        dfT=df.T
        newnames = dfT.columns.map(lambda x: datetime.date(datetime(int('20' + str(x.split(' ')[1])),self.months[x.split(' ')[0]],calendar.monthrange(int('20' + str(x.split(' ')[1])),self.months[x.split(' ')[0]])[1])).isoformat()) #convert index to datetime.date with last day of the month
        dfT = dfT.rename_axis(dict(zip(dfT.columns,newnames)),axis=0).sort_index(axis=0)
        df=dfT.T
        del df['Last Trade Date']
        del df['Last Trade Time']
        return df

    def update_warehouse(self,ota_or_ben): #first run
        df_key = str(str(self.last_datetime).split('.')[0][:-3].replace(':',''))
        if not os.path.isfile(self.warehouse_filename[ota_or_ben]):  #Create ASX HDF5 data file for the first time if it does not exist
            logger.info('Looks like this is the first time we have been run')
            warehouse = HDFStore(self.warehouse_filename[ota_or_ben],'a') #create the asx data warehouse!
            data = Panel({df_key:self.asx_futures[ota_or_ben]})
            warehouse[ota_or_ben] = data
            warehouse.close()      
        else:
            #logger.info('Opening and updating existing database @ ' + self.warehouse_filename[ota_or_ben])
            data = read_hdf(self.warehouse_filename[ota_or_ben],ota_or_ben) #open the asx data warehouse!
            data = data.join(Panel({df_key:self.asx_futures[ota_or_ben]}),how='outer') #join the new data to the exisiting panel data - this took a bit of figuring out. Outer is the union of the indexes so when a new row appears for a new quater, Nulls or NaNs full the remainder of the dataframe
            data.to_hdf(self.warehouse_filename[ota_or_ben],ota_or_ben) #open the asx data warehouse!
            #warehouse[ota_or_ben] = data      #overwrite ota_xxx
            #warehouse.close()      
            #There is an error in xlxw that returns a ValueError: More than 4094 XFs (styles) with greater than ~1350 sheets
            #For now we will limit the number of sheets to this. 
            logger.info('Saving data to ' + self.asx_path_P + ota_or_ben + '.xls')
            data.loc[data.items[-1350:]].to_excel(self.asx_path_P + ota_or_ben + '.xls') #spit to ASX_daily on P drive
            data.loc[data.items[-1350:]].to_excel(self.asx_path + ota_or_ben + '.xls') #spit to local

        self.asx_futures[ota_or_ben].to_csv(self.last_check[ota_or_ben])
            
    def update_if_data_changed(self,ota_or_ben,auto_manual):
        if auto_manual == 'auto':
            live = self.string_convert(self.asx_futures[ota_or_ben])
            last_check = self.read_last(self.last_check[ota_or_ben])
            #compare strings
            if live == last_check:
                logger.info('No change in NZ Electricity Futures at ' + ota_or_ben + ' since last check')
            else:
                logger.info('***Changes detected*** in NZ Electricity Futures at ' + ota_or_ben + ', appending DataFrame')
                self.last_datetime = datetime.now()
                self.update_warehouse(ota_or_ben)    
                #save most recent     
                #ben.to_csv(self.last_check[ota_ben])
        if auto_manual == 'manual':
                logger.info('***Manual update*** in NZ Electricity Futures at ' + ota_or_ben + ', appending DataFrame')
                self.last_datetime = datetime.now()
                self.update_warehouse(ota_or_ben)


    def get_asx_spreads(self):
        self.scrape_data()
        self.update_if_data_changed('Otahuhu','auto')
        self.update_if_data_changed('Benmore','auto')

    #Implement a manual upgrade (in case of open windows files)
    def get_manual_asx_spreads(self):
        self.scrape_data()
        self.update_if_data_changed('Otahuhu','manual')
        self.update_if_data_changed('Benmore','manual')

if __name__ == '__main__':
   asx_grab = asx_spreads_grabber()
   #axsdata = asx_grab.get_manual_asx_spreads()    
   axsdata = asx_grab.get_asx_spreads()    

