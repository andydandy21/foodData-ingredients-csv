import boto3
import pandas as pd

from decimal import Decimal


# load data
food_df = pd.read_csv('./rawData/food.csv')\
    .drop(["data_type", "food_category_id", "publication_date"], axis=1)\
    .dropna()
food_nutrient_df = pd.read_csv('./rawData/food_nutrient.csv')\
    .drop(["id", "data_points", "derivation_id", "min", "max", "median", "footnote", "min_year_acquired"], axis=1)\
    .dropna()
nutrient_df = pd.read_csv('./rawData/nutrient.csv')\
    .drop(["id", "rank"], axis=1)\
    .rename(columns={"nutrient_nbr": "nutrient_id"})\
    .dropna()

# merge nutrients
nutrient_df["nutrient_id"] = nutrient_df["nutrient_id"].astype("int16")
merged_nutrient = food_nutrient_df.merge(nutrient_df, how="inner", on=["nutrient_id"])\
     .drop(["nutrient_id"], axis=1)

# drop empty amounts
mask = merged_nutrient['amount'] == 0.0
merged_nutrient = merged_nutrient[~mask]

# combine columns and merge with food_df
merged_nutrient["nutrients"] = merged_nutrient.apply(lambda row: {
    "name": row["name"],
    "amount": Decimal(str(round(row["amount"], 2))),
    "unit": row["unit_name"]
}, axis=1)
merged_nutrient = merged_nutrient.drop(["name", "unit_name", "amount"], axis=1)
merged_nutrient = merged_nutrient.groupby(["fdc_id"]).agg(lambda x: x.tolist())
merged_food = food_df.merge(merged_nutrient, how="inner", on=["fdc_id"])

# prep for upload to dynamodb
merged_food = merged_food.rename(columns={"fdc_id": "SK", "description": "PK"})
merged_food["SK"] = merged_food["SK"].apply(lambda value: "FOOD#{}".format(value))

# export csv file
food_dict = merged_food.to_csv('ingredient_list.csv')
