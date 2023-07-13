import pandas as pd
import math
import sqlite3
from datetime import datetime

conn = sqlite3.connect("resupply_db")
c = conn.cursor()

c.execute(
    "CREATE TABLE IF NOT EXISTS dim_dishes (dish_id text, dish_name text, dish_type text)"
)
c.execute(
    "CREATE TABLE IF NOT EXISTS dim_items (item_id text, item_category text, item_name text, transaction_unit text, consumption_unit text)"
)
c.execute(
    "CREATE TABLE IF NOT EXISTS map_dish_item (dish_id text, item_id text, item_quantity real, item_unit text)"
)
conn.commit()


dim_dishes_df = pd.read_csv("./config/dim_dishes.csv")
dim_items_df = pd.read_csv("./config/dim_items.csv")
map_dish_item_df = pd.read_csv("./config/map_dish_item.csv")


dim_dishes_df.to_sql("dim_dishes", conn, if_exists="replace", index=False)
dim_items_df.to_sql("dim_items", conn, if_exists="replace", index=False)
map_dish_item_df.to_sql("map_dish_item", conn, if_exists="replace", index=False)

# with open("./config/dishes_tmp.txt", "r") as file:
#     selected_dish = file.read()

excel_data_df = pd.read_excel("grocery_planner.xlsx", sheet_name="planner")
meals_dict = excel_data_df.to_dict()
meals_dict.pop("Day")
meals_planned = []
# breakfast = list(meals_dict["Breakfast"].values())
# new_list = [item for item in breakfast if not (pd.isnull(item)) == True]
# print(new_list)
for key in meals_dict:
    # print("key=", key)
    raw_list = list(meals_dict[key].values())
    parsed_list = [item for item in raw_list if not (pd.isnull(item)) == True]
    # print(parsed_list)
    meals_planned.append(parsed_list)
# print(meals_planned)
meals_planned_parsed = [j for sub in meals_planned for j in sub]
# meals_planned_parsed = str(sum(meals_planned, []))
# print(len(meals_planned_parsed))

master_list = []

# for i in meals_planned_parsed:
#     print(i)

for dish in meals_planned_parsed:
    # print(i)
    selected_dish = dish

    query1 = "select dish_id from dim_dishes where dish_name='" + selected_dish + "'"
    # print(query1)
    c.execute(query1)
    selected_dish_id = c.fetchall()[0][0]
    query2 = (
        "select item_id,item_quantity,item_unit from map_dish_item where dish_id='"
        + selected_dish_id
        + "'"
    )

    c.execute(query2)
    # print(c.fetchall()[0][0])

    dish_item_list = []

    for row in c.fetchall():
        # print(row)
        dish_item_list.append(row)

    # print(dish_item_list)
    master_list.append(dish_item_list)

# print(master_list[0])

parsed_list = []
for ing in master_list[0]:
    parsed_list.append(list(ing))


def table_add(master, parsed):
    for dish in master:
        # print("dish in master", dish)
        for ingr in dish:
            match_flag = 0
            # print("ingr in master", ingr)
            ingr_id = ingr[0]
            # print("ingr_id in master", ingr_id)
            for ing1 in parsed:
                # print("ing1-id in parsed", ing1[0])
                if ingr_id == ing1[0]:
                    # print(f"ingredient:{ingr_id} from master dish {dish} matched")
                    match_flag = 1
                    ing1[1] += ing[1]
            if not match_flag:
                parsed_list.append(list(ingr))

    # master[0 to l-1][0] id to check
    # parsed_list[0-l dish][0-l ing][0] id checking


table_add(master_list[1:], parsed_list)
# print(len(parsed_list))
final_df = pd.DataFrame(columns=["Section", "Name", "Quantity", "Unit"])
for ingr in parsed_list:
    # print(ingr)
    query3 = (
        "select item_name, item_category from dim_items where item_id='" + ingr[0] + "'"
    )
    # print(query3)
    c.execute(query3)
    fetch_result = c.fetchall()[0]
    name_item = fetch_result[0]
    section_item = fetch_result[1]

    final_df.loc[len(final_df.index)] = [section_item, name_item, ingr[1], ingr[2]]

final_df = final_df.sort_values("Section")

# print(final_df)
# print(len(str(datetime.now())))


def get_timestamp():
    str1 = []
    for i in str(datetime.now()):
        try:
            str1.append(int(i))
        except:
            pass

    time_stamp = ""

    for ele in str1:
        time_stamp += str(ele)
    return str(time_stamp)


file_name = "grocery_list_" + get_timestamp()
final_df.to_csv(f"./output/{file_name}.csv", index=False)
