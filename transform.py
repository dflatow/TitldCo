import pandas as pd
import MySQLdb
from data_helpers import *
from datetime import datetime

CON = MySQLdb.connect(host='198.71.51.98',
                      port=3306,
                      user='admin_user',
                      passwd='gamal1980',
                      db='titld')

CON_DATA = MySQLdb.connect(host='198.71.51.98',
                           port=3306,
                           user='admin_user',
                           passwd='gamal1980',
                           db='titld_data')


if __name__ == "__main__":

    tables = ["Facebook", "Instagram", "InstagramTag", "Twitter", "Wikipedia"]
    query_format = "SELECT * FROM `%s`"
    dfs = [pd.read_sql_query(query_format % (table,),
                           CON_DATA,
                           index_col=None,
                           coerce_float=True,
                           params=None,
                           parse_dates=None,
                           chunksize=None).set_index("post_id") for table in tables]
    
    df = pd.concat(dfs, axis=1)
    df = df[df.columns.unique()]
    #df[["fb_likes", "fb_talking_about_count","ig_followed_by","ig_follows","ig_media","ig_tag_count","tw_followers","tw_friends_count","tw_statuses_count","wi_page_views", "ts"]]
    df = df.sort("ts", ascending=True)
    change = df.iloc[-1] - df[-2]
    new_df = df.join(change, how="outer", rsuffix="_change")
    print new_df

