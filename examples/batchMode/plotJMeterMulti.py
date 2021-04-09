#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import os
import re
import numpy as np

import seaborn as sns


'''
This function filters out all the rows for which the label column does not match a given value (i.e GetDistribution).
And saves "elapsed", "responsecode" & "timestamp" value for rows that do match the specified label text(transaction name) into a csv.
'''
LOCATION = os.getcwd()
TRANSACTION_NAME = 'HTTP Request'  #replace the transaction name with your transaction name

def extract_data():

    FILE_TO_WRITE ="./merged.csv"
    lookup_df = pd.read_csv('./mapConfig.csv')

     
    if os.path.exists(FILE_TO_WRITE):
        os.remove(FILE_TO_WRITE)
    fwrite = open(FILE_TO_WRITE,'a') 
    fwrite.write('timestamp,location,latency,responsecode\n')       
    for file in os.listdir(LOCATION):
        try: #extract data from csv files that begin with TestPlan_
            if file.startswith("TestPlan_") and file.endswith(".csv"):
                substring = re.search('TestPlan_results_(.+?).csv', file).group(1)
                y = lookup_df.loc[lookup_df[' Filename'].str.contains(substring)][' Region']
                y = y.values.tolist()
                #print(y[0].lstrip())
                if len(y)>0:  # only proceed if the file is listed in mapConfig.csv
                    # print("y = %s" % y)
                    df = pd.read_csv(file)
                    x = []
                    x = df.loc[df['label'] == TRANSACTION_NAME] #filter out all the rows for which the label column does not contain value GetDistribution
                    # print("file = %s   len(x) = %i" %(file,len(x)))
                    for item in range(len(x)):
                        truncatedResponseCode = x['responseCode'].values[item]
                        if isinstance(truncatedResponseCode, str):
                            truncatedResponseCode = 599
                        fwrite.write('%s,%s,%s,%s\n' %(x['timeStamp'].values[item],y[0],x['elapsed'].values[item],truncatedResponseCode))
            else:
                continue
        except Exception as e:
            raise e

def generate_graphs():
    FILE = "./merged.csv" #replace with your file name
     
    try:
        df = pd.read_csv(FILE) # read the file
                
        res = df.pivot(columns='location', values='latency')
        
        fig, axes = plt.subplots(3, 2, figsize=(14, 10), sharey=False) # set 2x2 plots
        
        # fig.patch.set_facecolor('xkcd:mint green')
        fig.patch.set_facecolor('#bbe5f9')
        plt.subplots_adjust(hspace = 0.3)
        color = {' USA':'#0000FF',' Russia':'#FF0000',' Other':'#00FF00' }
        
        #generate scatterplot for elapsed time , palette="deep",style=df['location']
        ax = sns.scatterplot(ax=axes[0,0], data=df,x=df['timestamp'],y=df['latency'], hue=df['location'], palette=color, s=7, legend=True) #palette="deep"
        ax.set(ylim=(0,3000))
        xticks = ax.get_xticks()
        ax.set_xticklabels([pd.to_datetime(tm, unit='ms').strftime('%H:%M:%S') for tm in xticks]) #, rotation = 45
        ax.legend(fontsize='medium')
        ax.set_title('Response Time Over Time')
        ax.set_xlabel('Time')
        ax.set_ylabel('Response Time (ms)')
        
        
        #generate response time distribution graph
        kwargs = dict(element='step',shrink=.8, alpha=0.3, fill=True, legend=True, palette=color) 
        ax = sns.histplot(ax=axes[0, 1], data=res,**kwargs)
        ax.set(xlim=(0,3000))
        ax.set_title('Response Time Distribution')
        ax.set_xlabel('Response Time (ms)')
        ax.set_ylabel('Frequency')
        
        
        #generate latency/response time basic statistics 
        axes[1, 0].axis("off")
        
        summary = np.round(res.describe(percentiles=[0.25,0.5,0.75,0.90,0.95],include='all'),2)# show basic statistics as in row
        table_result = axes[1, 0].table(cellText=summary.values,
                  rowLabels=summary.index,
                  colLabels=summary.columns,
                  cellLoc = 'right', rowLoc = 'center',
                  loc='center')
        table_result.auto_set_font_size(False)
        table_result.set_fontsize(9)
        #axes[1, 0].set_title('Response Time Statistics')
        
        #generate percentile distribution       
        summary = np.round(res.describe(percentiles=[0.0, 0.1, 0.2,
                                                         0.3, 0.4, 0.5,
                                                         0.6, 0.7, 0.8,  
                                                         0.9, 0.95, 0.99, 1]),2) # add 1 in the percentile
        dropping = ['count', 'mean', 'std', 'min','max'] #remove metrics not needed for percentile graph
        
        for drop in dropping:
            summary = summary.drop(drop)        
        ax = sns.lineplot(ax=axes[1, 1],data=summary,dashes=False, palette=color, legend=True)
        ax.legend(fontsize='medium')
        ax.set(ylim=(0,6000))
        ax.set_title('Percentile Distribution')
        ax.set_xlabel('Percentile')
        ax.set_ylabel('Response Time (ms)')

        #generate response code scatterplot ,palette="deep",style=df['location']
        ax = sns.scatterplot(ax=axes[2, 0], data=df,x=df['timestamp'],y=df['responsecode'], hue=df['location'], palette=color, s=10, legend=True)
        ax.set(ylim=(0,600))
        ax.legend(fontsize='medium')
        ax.set_title('Response Code Over Time')
        xticks = ax.get_xticks()
        ax.set_xticklabels([pd.to_datetime(tm, unit='ms').strftime('%H:%M:%S') for tm in xticks]) #, rotation = 45
        ax.set_xlabel('Time')
        ax.set_ylabel('Response Code')
        
        #generate response code % distribution barplot
        resp_code = df[['location','responsecode']].copy()
        res_code = resp_code.pivot_table(index='location', columns='responsecode', aggfunc=len)
        res_code = res_code.unstack(level=0)
        percents_df= np.round(res_code.groupby('location').apply(lambda x: 100 * x / x.sum()),2).reset_index()     
        ax = sns.barplot(ax=axes[2, 1],data=percents_df,x=0, y='responsecode', hue='location',palette=color, orient = 'h')
        #ax.legend_.remove()
        ax.legend().set_title('')
        ax.set_title('Response Code - % Distribution')
        ax.set_xlabel('% Distribution')
        ax.set_ylabel('Response Code')
             
        fig.tight_layout(pad=2)  

        plt.savefig('graphs.png',facecolor=fig.get_facecolor(), edgecolor='none')
    except Exception as e:
        raise e  

def main():
    extract_data()
    generate_graphs()
    

if __name__ == "__main__":
    main()
