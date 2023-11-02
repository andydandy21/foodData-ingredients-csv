import pandas as pd

from decimal import Decimal


def create_conversion_factor(row):
    # util function to be used later:
    # nutrients are measured per 100 grams, this provides a factor to measure nutrients per 1 cup (if available)
    if row['portion_description'] == '1 cup' or \
            row['portion_description'] == '1 cup, melted' or \
            row['portion_description'] == '1 cup, canned' or \
            row['portion_description'] == '1 cup, beef flavor' or \
            row['portion_description'] == '1 cup, chicken flavor':
        return row['gram_weight'] / 100
    elif row['portion_description'] == '1 fl oz' or row['portion_description'] == '1 fl oz (no ice)':
        return row['gram_weight'] * 8 / 100
    elif row['portion_description'] == '1 tablespoon':
        return row['gram_weight'] * 16 / 100
    elif row['portion_description'] == '1 teaspoon' or \
            row['portion_description'] == '1 teaspoon, dry' or \
            row['portion_description'] == '1 teaspoon, NFS':
        return row['gram_weight'] * 48 / 100


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
food_portion = pd.read_csv('./rawData/food_portion.csv') \
    .drop(['id', 'seq_num', 'amount', 'measure_unit_id', 'modifier'], axis=1) \
    .dropna()
test = pd.read_csv('./rawData/food_portion.csv') \
    .drop(['id', 'seq_num', 'amount', 'measure_unit_id', 'modifier'], axis=1) \
    .dropna()

# keep food portions if they have a liquid conversion and sort
food_portion = food_portion.loc[food_portion['portion_description'].isin([
    '1 fl oz', '1 fl oz (no ice)', '1 cup', '1 cup, melted', '1 cup, canned', '1 cup, chicken flavor',
    '1 cup, beef flavor', '1 cup, canned', '1 tablespoon', '1 teaspoon', '1 teaspoon (dry)',
])].sort_values('portion_description').drop_duplicates(subset='fdc_id', keep='first')

# add conversion factor row and drop now-unneeded rows
food_portion['liquid_conversion_factor'] = food_portion.apply(create_conversion_factor, axis=1)
food_portion = food_portion.drop(['portion_description', 'gram_weight'], axis=1)

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
merged_food = merged_food.merge(food_portion, how="outer", on=["fdc_id"])

# provide a default value for our conversion factor if it doesn't exist
merged_food['liquid_conversion_factor'].fillna(0, inplace=True)

non_conv = merged_food.loc[merged_food['liquid_conversion_factor'] == 0]
non_arr = non_conv['fdc_id'].to_list()

# prep for upload to dynamodb
merged_food = merged_food.rename(columns={"fdc_id": "SK", "description": "PK"})
merged_food["SK"] = merged_food["SK"].apply(lambda value: "FOOD#{}".format(value))

# export csv file
food_dict = merged_food.to_csv('ingredient_list.csv')

# uncomment to export as json
food_dict = merged_food.to_json('ingredient_list.json')
