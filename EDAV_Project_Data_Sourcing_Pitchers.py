#!/usr/bin/env python
# coding: utf-8

# In[2]:


# pip install pybaseball


# In[1]:


from pybaseball import pitching_stats
from pybaseball import batting_stats
import pandas as pd


# ### Importing pitching statistics via pybaseball library

# In[2]:


data_1 = pitching_stats(1911,1917, qual=40)
data_2 = pitching_stats(1918,1919, qual=30)
data_3 = pitching_stats(1920, qual=40)
data_4 = pitching_stats(1921,1930, qual=40)
data_5 = pitching_stats(1931,1940, qual=40)
data_6 = pitching_stats(1941,1950, qual=40)
data_7 = pitching_stats(1951,1960, qual=40)
data_8 = pitching_stats(1961,1970, qual=40)
data_9 = pitching_stats(1971, qual=40)
data_10 = pitching_stats(1972, qual=35)
data_11 = pitching_stats(1973,1980, qual=40)
data_12 = pitching_stats(1981, qual=20)
data_13 = pitching_stats(1982,1990, qual=40)
data_14 = pitching_stats(1991,1993, qual=40)
data_15 = pitching_stats(1994,1995, qual=30)
data_16 = pitching_stats(1996,2000, qual=40)
data_17 = pitching_stats(2001,2010, qual=40)
data_18 = pitching_stats(2011,2019, qual=40)
data_19 = pitching_stats(2020, qual=5)
data_20 = pitching_stats(2021,2025, qual=40)


# In[3]:


import pandas as pd
df = pd.concat([data_1, data_2, data_3, data_4, data_5, data_6, data_7, data_8, data_9, data_10, data_11, data_12,
               data_13, data_14, data_15, data_16, data_17, data_18, data_19, data_20], ignore_index=True)


# In[6]:


df.shape


# In[39]:


df.head()


# In[7]:


df.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/pitching_stats.csv')


# In[2]:


dfp = pd.read_csv('/Users/ryan/Desktop/STAT5702GR/final_project/pitching_stats.csv')


# In[3]:


dfp.head()


# ### Lahman Data

# In[4]:


df_awards = pd.read_csv('/Users/ryan/Desktop/STAT5702GR/final_project/AwardsPlayers.csv')
df_people = pd.read_csv('/Users/ryan/Desktop/STAT5702GR/final_project/People.csv')
df_teams = pd.read_csv('/Users/ryan/Desktop/STAT5702GR/final_project/Teams.csv')


# ### Translation_Table

# In[6]:


import warnings
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)


# In[7]:


from pathlib import Path

folder = Path("/Users/ryan/Desktop/STAT5702GR/final_project/ids")

# Get all CSV files
files = list(folder.glob("*.csv"))

# Read and concatenate
tr_table = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

print("Combined shape:", tr_table.shape)
tr_table.head()


# ### Merging Data

# Player ID translation file to merge the Lahman dataframes to the pybaseball dataframes

# In[28]:


import pandas as pd

# -------------------------------------------------------------------
# 1. Join Lahman awards (df_awards) to Lahman people (df_people)
# -------------------------------------------------------------------

# Keep only needed columns from Lahman People
people_cols = ["playerID", "bbrefID", "nameFirst", "nameLast"]
people_subset = df_people[people_cols].copy()

# Merge awards with people on playerID
lahman_awards = df_awards.merge(
    people_subset,
    on="playerID",
    how="left",
    validate="m:1"   # many awards → one people record
)

# Optional: create full name (purely cosmetic)
lahman_awards["full_name"] = (
    lahman_awards["nameFirst"].fillna("") + " " +
    lahman_awards["nameLast"].fillna("")
).str.strip()


# -------------------------------------------------------------------
# 2. Use translation table (tr_table) to attach FanGraphs ID to Lahman
# -------------------------------------------------------------------

# Ensure IDs are strings
lahman_awards["bbrefID"]      = lahman_awards["bbrefID"].astype(str)
tr_table["key_bbref"]         = tr_table["key_bbref"].astype(str)
# tr_table["key_fangraphs"]     = tr_table["key_fangraphs"].astype(str)
# dfp["IDfg"]                   = dfp["IDfg"].astype(str)

# Deduplicate translation table on bbref key (safety measure)
tr_table_dedup = tr_table.drop_duplicates(subset=["key_bbref"]).copy()

# Attach FanGraphs ID to Lahman awards using bbrefID → key_bbref
lahman_awards_fg = lahman_awards.merge(
    tr_table_dedup[["key_bbref", "key_fangraphs"]],
    left_on="bbrefID",
    right_on="key_bbref",
    how="left",
    validate="m:1"
)


# -------------------------------------------------------------------
# 3. Merge Lahman(+awards) with FanGraphs batting data on BOTH:
#       key_fangraphs ↔ IDfg   (player)
#       yearID        ↔ Season (year)
# -------------------------------------------------------------------

# Standardize year types
lahman_awards_fg["yearID"] = lahman_awards_fg["yearID"].astype(int)
dfp["Season"]              = dfp["Season"].astype(int)  # rename if needed

awards_pitching = lahman_awards_fg.merge(
    dfp,
    left_on=["key_fangraphs", "yearID"],
    right_on=["IDfg", "Season"],
    how="left",
    validate="m:1"   # each award-year should match at most one batting row
)


# -------------------------------------------------------------------
# 4. (OPTIONAL) You may choose to limit the columns later.
#     The block below is left commented out so you can customize.
# -------------------------------------------------------------------


# Example: columns you *might* want to select later
cols_to_keep = [
    "playerID",
    "bbrefID",
    "full_name",
    "awardID",
    "yearID",
    "lgID",
    "key_fangraphs",   # FanGraphs ID
    "IDfg",
    "W", "L", "ERA", "GS", "G", "CG", "SHO", "SV", "IP", "SO", "K/9", "WHIP", "FIP", "SwStr%", "ERA-", "FIP-",
    "K%", "BB%", "K/9+", "K/BB+", "WHIP+", "AVG+", "K%+", "Hard%+", "Soft%+", "EV", "LA", "Barrel%", "WAR"
    # FG ID in dfp (same as key_fangraphs)
    # Add any batting stat columns here, e.g.:
    # "WAR", "G", "PA", "HR"
]

# # Only keep columns that actually exist
cols_to_keep = [c for c in cols_to_keep if c in awards_pitching.columns]

awards_pitching = awards_pitching[cols_to_keep].copy()

awards_pitching.head()


# If you skip filtering for now, awards_pitching is your full merged dataset:
# print(awards_pitching.head())



# In[30]:


import pandas as pd

# 1. Load your datasets
# Replace 'teams.csv' and 'batters.csv' with your actual file paths

# 2. Merge Batters with Teams to get the League ID
# We assume the batters dataset has a 'teamID' column to link with the teams dataset.

dfp['WAR'] = pd.to_numeric(dfp['WAR'], errors='coerce')

merged_df = pd.merge(
    dfp, 
    df_teams[['yearID', 'franchID', 'lgID']], 
    left_on=['Season', 'Team'],
    right_on=['yearID', 'franchID'],
    how='left'
)

# 3. Aggregate WAR by Player, Year, and League
# This step handles cases where a player played for multiple teams 
# within the same league in a single year (summing their WAR).
player_league_war = merged_df.groupby(['yearID', 'lgID', 'IDfg'], as_index=False)['WAR'].sum()

# 4. Calculate the Max WAR for each Year and League
# We transform the data to add a column with the max WAR for that group
player_league_war['max_WAR'] = player_league_war.groupby(['yearID', 'lgID'])['WAR'].transform('max')

# 5. Create the WAR Leader Flag
# Flag is True (or 1) if the player's WAR equals the max WAR for that league/year
player_league_war['WAR_leader_flag'] = (player_league_war['WAR'] == player_league_war['max_WAR']).astype(int)

# 6. Select and Rename Columns for Final Output
output_df = player_league_war[['IDfg', 'yearID', 'lgID', 'WAR_leader_flag']].copy()
output_df.columns = ['IDfg', 'year', 'league', 'WAR leader flag']

# Display the first few rows
print(output_df.head())

# Optional: Save to a new CSV
output_df.to_csv('war_leaders.csv', index=False)


# In[31]:


player_league_war['WAR'].dtype


# In[32]:


war_leaders = output_df.copy()


# In[33]:


war_leaders


# In[34]:


war_leaders['year'] = war_leaders['year'].astype('int64')
awards_pitching['yearID'] = awards_pitching['yearID'].astype('int64')


# In[35]:


import numpy as np
import pandas as pd

# clean the strings first
awards_pitching['key_fangraphs'] = (
    awards_pitching['key_fangraphs']
      .astype(str)
      .str.strip()
      .str.replace('.0', '', regex=False)
)

# turn impossible values into NaN, then use nullable Int64
awards_pitching['key_fangraphs'] = pd.to_numeric(
    awards_pitching['key_fangraphs'],
    errors='coerce'        # 'nan', '', junk -> NaN
).astype('Int64')          # pandas nullable integer dtype


# In[36]:


awards_pitching


# In[37]:


pitching_final = pd.merge(
    awards_pitching,
    war_leaders,
    left_on=['key_fangraphs', 'yearID'],
    right_on=['IDfg', 'year'],
    how='left'
)


# In[38]:


pitching_final.loc[batting_final['key_fangraphs']== 2036]


# In[39]:


cy = pitching_final.loc[pitching_final['awardID'] == 'Cy Young Award']


# In[40]:


cy


# In[41]:


cy.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/cy_young_pitching_dataset_rev.csv')


# In[42]:


pitching_final.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/pitching_dataset_all.csv')


# In[ ]:




