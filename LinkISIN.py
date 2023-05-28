import pandas as pd
import gspread
import numpy as np
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('Data_Source')
news_sheet=gs.worksheet('NewsData')
details_sheet=gs.worksheet('AllDetails')
all_record_details=details_sheet.get_all_records()
details_df=pd.DataFrame(all_record_details)
all_record_news=news_sheet.get_all_records()
news_df=pd.DataFrame(all_record_news)
tag_list = list(details_df["Tag"])
name_list = list(details_df["Name"])
isin_list = list(details_df["ISIN"])
sub_list = list(details_df["Sub Tag"])

news_df['Match Stock'] = ""
news_df['ISIN'] = ""

for index, row in news_df.iterrows():
    headline = row['Headline']
    matching_tag = ""
    matching_isin = ""

    if pd.notnull(headline):
        for tag, name, sub, isin in zip(tag_list, name_list, sub_list, isin_list):
            if pd.notnull(tag) and str(tag) in str(headline):
                matching_tag = tag
                matching_isin = isin
                break
            if pd.notnull(name):
                stock_name_without_ltd = name.replace(" Ltd", "")
                if stock_name_without_ltd in str(headline):
                    matching_tag = name
                    matching_isin = isin
                    break

        # Check sub-tags if name or tag not found
        if not matching_tag:
            for tag, name, sub, isin in zip(tag_list, name_list, sub_list, isin_list):
                if pd.notnull(sub) and sub != "" and str(sub) in str(headline):
                    matching_tag = tag
                    matching_isin = isin
                    break
                if pd.notnull(name):
                    stock_name_without_ltd = name.replace(" Ltd", "")
                    if stock_name_without_ltd in str(headline):
                        matching_tag = name
                        matching_isin = isin
                        break

    news_df.at[index, 'Match Stock'] = matching_tag
    news_df.at[index, 'ISIN'] = matching_isin
column_name = ''  
if column_name and column_name in news_df.columns:
    news_df = news_df.drop(column_name, axis=1)
gsnew = client.open('Data_Source')
main_sheet=gsnew.worksheet('NewsLink')
main_sheet.clear()
set_with_dataframe(main_sheet,news_df)
