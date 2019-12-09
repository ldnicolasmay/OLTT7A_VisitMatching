#!/usr/bin/env python

import pandas as pd
import numpy as np
import re
import os
import os.path
import datetime
import d6tjoin.top1

import helpers as hlps
import config as cfg



df_u3 = hlps.export_redcap_records(uri=cfg.REDCAP_API_URI,
                                   token=cfg.REDCAP_API_TOKEN_UDS3n,
                                   fields="ptid,form_date")

ptrn_ptid = re.compile(r'^UM\d{8}$')
has_verified_id = df_u3['ptid'].str.match(ptrn_ptid)
has_form_date = df_u3['form_date'].notnull()

df_u3_cln = \
    df_u3.loc[has_verified_id & has_form_date, ['ptid', 'form_date', 'redcap_event_name']].reset_index(drop=True)
df_u3_cln['form_date'] = pd.to_datetime(df_u3_cln['form_date'])

ptrn_visitnum = re.compile(r'(\d+)')


def get_visit_num(visit_str):
    r = re.search(ptrn_visitnum, visit_str)
    visit_num = r.group(1)
    return int(visit_num)


def adjust_visit_num(ptid, visit_num):
    if "UM00000543" <= ptid <= "UM00001041":
        visit_num = visit_num - 1
    adj_visit_num = "Visit " + "0"*(3-len(str(visit_num))) + str(visit_num)
    return adj_visit_num


df_u3_cln_mut = df_u3_cln.copy()
df_u3_cln_mut['visit_num'] = df_u3_cln_mut['redcap_event_name'].apply(get_visit_num)
df_u3_cln_mut['visit_num_adj'] = \
    df_u3_cln_mut.apply(lambda df: adjust_visit_num(df['ptid'], df['visit_num']), axis='columns')

df_u3_cln_mut = df_u3_cln_mut.sort_values(['ptid', 'form_date'])


oltt_path = "/Users/ldmay/Box/Documents/OLTTDataScrape/OLTT Data/"
ptrn_cuedrcl = re.compile(r'^\d{3,4}-Cued Recall-\w{14}.csv$', re.IGNORECASE)
ptrn_ptid = re.compile(r'^(\d+)')


def get_ptid(file):
    r = re.match(ptrn_ptid, file)
    ptid_digits = r.group(1)
    return "UM" + "0" * (8 - len(ptid_digits)) + ptid_digits


def get_oltt_file_date(filepath):
    t = os.path.getmtime(filepath)
    return datetime.datetime.fromtimestamp(t)


df_files = pd.DataFrame.from_dict({
    'ptid': [],
    'oltt_file_date': [],
    'oltt_file_path': []
})

for path_dirs_files in os.walk(oltt_path):
    # print(path_dirs_files)
    path, dirs, files = path_dirs_files[0], path_dirs_files[1], path_dirs_files[2]

    for file in files:
        if re.match(ptrn_cuedrcl, file):
            ptid_temp = get_ptid(file)
            oltt_file_date_temp = get_oltt_file_date(path + "/" + file)

            df_files_temp = pd.DataFrame.from_dict({
                'ptid': [ptid_temp],
                'oltt_file_date': [oltt_file_date_temp],
                'oltt_file_path': [path + "/" + file]
            })

            df_files = df_files.append(df_files_temp, ignore_index=True)


df_files_srt = df_files.sort_values(['ptid', 'oltt_file_date'])

fuzzy_result = d6tjoin.top1.MergeTop1(df_u3_cln_mut, df_files_srt,
                                      exact_left_on=['ptid'], exact_right_on=['ptid'],
                                      fuzzy_left_on=['form_date'], fuzzy_right_on=['oltt_file_date']).merge()

# fuzzy_result['top1']
# fuzzy_result['top1']['form_date']
df_fuzzy_date = fuzzy_result['top1']['form_date']
df_fuzzy_date_srt = df_fuzzy_date.sort_values(['ptid', '__top1left__']).reset_index(drop=True) # 0-714

df_merged = fuzzy_result['merged']
df_merged_srt = df_merged.sort_values(['ptid', 'form_date'])

df_merged_fuzzy_date = \
    pd.merge(df_merged_srt, df_fuzzy_date_srt,
             how="left", left_on=['ptid', 'form_date'], right_on=['ptid', '__top1left__'])

# df_merged_fuzzy_date['__top1diff__'] < np.timedelta64(365, 'D')
# df_merged_fuzzy_date['__top1left__'] <= df_merged_fuzzy_date['__top1right__']
s_bool_lte180Days = df_merged_fuzzy_date['__top1diff__'] < np.timedelta64(365, 'D')
s_bool_visitLteFile = df_merged_fuzzy_date['__top1left__'] <= df_merged_fuzzy_date['__top1right__']

df_merged_fuzzy_date_flt = df_merged_fuzzy_date.loc[s_bool_lte180Days & s_bool_visitLteFile, :].reset_index(drop=True)


def clean_oltt_file_path(oltt_file_path):
    foo = re.sub(r'/Users/ldmay/Box/Documents/OLTTDataScrape/', "", oltt_file_path)
    bar = re.sub(r'\d{3,4}-Cued Recall-\w{14}.csv', "", foo)
    return bar


df_merged_fuzzy_date_flt['oltt_file_path'] = df_merged_fuzzy_date_flt['oltt_file_path'].apply(clean_oltt_file_path)

cols_to_keep = ['ptid', 'form_date', 'oltt_file_path', 'oltt_file_date', 'visit_num_adj',
                '__top1left__', '__top1right__', '__top1diff__']

df_merged_fuzzy_date_flt[cols_to_keep].to_csv("OLTT_visit_number_help.csv", index=False)
