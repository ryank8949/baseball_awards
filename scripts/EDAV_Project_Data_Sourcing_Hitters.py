#!/usr/bin/env python
# coding: utf-8

# In[2]:


# pip install pybaseball


# In[1]:


from pybaseball import pitching_stats
from pybaseball import batting_stats
import pandas as pd


# ### Importing hitting statistics via pybaseball library

# In[9]:


data_1b = batting_stats(1911,1917, qual=300)
data_2b = batting_stats(1918,1919, qual=200)
data_3b = batting_stats(1920, qual=300)
data_4b = batting_stats(1921,1930, qual=300)
data_5b = batting_stats(1931,1940, qual=300)
data_6b = batting_stats(1941,1950, qual=300)
data_7b = batting_stats(1951,1960, qual=300)
data_8b = batting_stats(1961,1970, qual=300)
data_9b = batting_stats(1971, qual=300)
data_10b = batting_stats(1972, qual=250)
data_11b = batting_stats(1973,1980, qual=300)
data_12b = batting_stats(1981, qual=200)
data_13b = batting_stats(1982,1990, qual=300)
data_14b = batting_stats(1991,1993, qual=300)
data_15b = batting_stats(1994,1995, qual=200)
data_16b = batting_stats(1996,2000, qual=300)
data_17b = batting_stats(2001,2010, qual=300)
data_18b = batting_stats(2011,2019, qual=300)
data_19b = batting_stats(2020, qual=80)
data_20b = batting_stats(2021,2025, qual=300)


# In[16]:


import pandas as pd
dfb = pd.concat([data_1b, data_2b, data_3b, data_4b, data_5b, data_6b, data_7b, data_8b, data_9b, data_10b, data_11b, data_12b,
                data_13b, data_14b, data_15b, data_16b, data_17b, data_18b, data_19b, data_20b], ignore_index=True)



# In[2]:


dfb.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/batting_stats.csv')


# In[3]:


dfb.shape


# In[2]:


dfb = pd.read_csv('/Users/ryan/Desktop/STAT5702GR/final_project/batting_stats.csv')


# In[3]:


dfb.head()


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

# In[8]:


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
# dfb["IDfg"]                   = dfb["IDfg"].astype(str)

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
dfb["Season"] = dfb["Season"].astype(int)  # rename if needed

awards_batting = lahman_awards_fg.merge(
    dfb,
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
    "HR", "AVG", "RBI", "SO", "OBP", "SLG", "OPS", "wOBA", "wRC", "wRC+", "WAR", "Hard%", "OBP+", "SLG+", "ISO+", "BABIP+",
    "Hard%+", "EV", "LA", "Barrels", "Barrel%", "maxEV", "HardHit", "HardHit%", "Events", "G", "PA", "H"
    # FG ID in dfb (same as key_fangraphs)
    # Add any batting stat columns here, e.g.:
    # "WAR", "G", "PA", "HR"
]

# Only keep columns that actually exist
cols_to_keep = [c for c in cols_to_keep if c in awards_batting.columns]

awards_batting = awards_batting[cols_to_keep].copy()

awards_batting.head()


# If you skip filtering for now, awards_batting is your full merged dataset:
# print(awards_batting.head())



# In[9]:


import pandas as pd

# 1. Load your datasets
# Replace 'teams.csv' and 'batters.csv' with your actual file paths

# 2. Merge Batters with Teams to get the League ID
# We assume the batters dataset has a 'teamID' column to link with the teams dataset.

dfb['WAR'] = pd.to_numeric(dfb['WAR'], errors='coerce')

merged_df = pd.merge(
    dfb, 
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


# In[10]:


player_league_war['WAR'].dtype


# In[12]:


merged_df.loc[merged_df['IDfg']==15640]


# In[22]:


dfbj = merged_df.loc[merged_df['IDfg']==26289]
dfbj = dfbj[['Name', 'Season', 'WAR']]
dfbj


# In[53]:


player_league_war.loc[player_league_war['IDfg'] == '15640']


# In[13]:


war_leaders = output_df.copy()


# In[14]:


war_leaders


# In[15]:


war_leaders['year'] = war_leaders['year'].astype('int64')
awards_batting['yearID'] = awards_batting['yearID'].astype('int64')


# In[16]:


import numpy as np
import pandas as pd

# clean the strings first
awards_batting['key_fangraphs'] = (
    awards_batting['key_fangraphs']
      .astype(str)
      .str.strip()
      .str.replace('.0', '', regex=False)
)

# turn impossible values into NaN, then use nullable Int64
awards_batting['key_fangraphs'] = pd.to_numeric(
    awards_batting['key_fangraphs'],
    errors='coerce'        # 'nan', '', junk -> NaN
).astype('Int64')          # pandas nullable integer dtype


# In[17]:


awards_batting


# In[18]:


batting_final = pd.merge(
    awards_batting,
    war_leaders,
    left_on=['key_fangraphs', 'yearID'],
    right_on=['IDfg', 'year'],
    how='left'
)


# In[23]:


batting_final.loc[batting_final['key_fangraphs']== 1000001]


# In[42]:


awards_batting


# In[20]:


mvp = batting_final.loc[batting_final['awardID'] == 'Most Valuable Player']


# In[21]:


mvp


# In[22]:


mvp.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/mvp_hitting_dataset.csv')


# In[24]:


batting_final.to_csv('/Users/ryan/Desktop/STAT5702GR/final_project/hitting_dataset_all.csv')


# In[ ]:




