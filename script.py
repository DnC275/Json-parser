import json
import os
from constants import *
import pandas as pd


def set_general_params(tmp, data):
    general = data[GENERAL_INFO]
    tmp[DATE_BEGIN] = general.get(DATE_BEGIN, 'None')
    tmp[DATE_END] = general.get(DATE_END, 'None')
    tmp[PWC_CODE] = general.get(PWC_CODE, 'None')
    return tmp


def set_goods_params(tmp, goods):
    tmp[DISCOUNT_TYPE] = goods.get(DISCOUNT_TYPE, 'None')
    tmp[DISCOUNT_VALUE] = goods.get(DISCOUNT_VALUE, 'None')
    return tmp


def set_price_options(tmp, goods):
    if PRICE_OPTIONS in goods and goods[PRICE_OPTIONS]:
        price_options = goods[PRICE_OPTIONS]
        for options in price_options:
            tmp[FIRST_VALUE] = options.get(FIRST_VALUE, 'None')
            tmp[VALUE] = options.get(VALUE, 'None')
            operator = options.get(OPERATOR, '')
            if operator == LESS_OR_EQUAL:
                tmp[LESS_OR_EQUAL] = operator
            else:
                tmp[LESS_OR_EQUAL] = 'None'
    else:
        tmp[VALUE] = tmp[LESS_OR_EQUAL] = 'None'
    return tmp


def make_goods_composition(goods) -> dict:
    if GOODS_COMPOSITION in goods and goods[GOODS_COMPOSITION]:
        goods_composition = goods['GoodsComposition']
        composition_dict = dict()
        for composition in goods_composition:
            value = composition[VALUE]
            for code in composition[GOODS_CODE]:
                composition_dict[code] = value
        return composition_dict
    return dict()


if __name__ == '__main__':
    path = './resources/'
    files = os.listdir(path)

    my_columns = [ITEM, SALE_PRICE_BEFORE, SALE_PRICE_TIME, DATE_PRICE_BEFORE, OBJ_CODE, DISCOUNT_TYPE,
                  DISCOUNT_VALUE, DATE_BEGIN, DATE_END, PWC_CODE, VALUE, FIRST_VALUE, LESS_OR_EQUAL, FILE]

    general_columns = ['DateBegin', 'DateEnd', 'PWCcode']

    df = pd.DataFrame()
    for i, val in enumerate(my_columns, start=0):
        df.at[i, val] = ''

    k = 0
    for file in files:
        with open(path + file, 'r') as read_file:
            data = json.load(read_file)
            tmp_dict = {FILE: file}

            set_general_params(tmp_dict, data)

            goods_lists = data['Information']['GoodsLists']

            for goods in goods_lists:
                composition_value_dict = make_goods_composition(goods)

                tmp_dict['DiscountType'] = goods.get('DiscountType', 'None')
                tmp_dict['DiscountValue'] = goods.get('DiscountValue', 'None')

                set_price_options(tmp_dict, goods)

                for price in goods[PRICES]:
                    tmp_dict[OBJ_CODE] = price.get(STORE_CODE, 'None')
                    columns_name = price['ColumnsName']
                    price_data = price['Data']
                    for good in price_data:
                        if len(good) != len(columns_name):
                            raise Exception('Incorrect json file structure')

                        for i in range(len(columns_name)):
                            tmp_dict[columns_name[i]] = good[i]

                        if tmp_dict[ITEM] in composition_value_dict:
                            tmp_dict[VALUE] = composition_value_dict[tmp_dict[ITEM]]

                        for key in my_columns:
                            df.at[k, key] = tmp_dict[key] if tmp_dict[key] else 'None'
                        k += 1

    df.to_excel('./result.xlsx', index=False)
