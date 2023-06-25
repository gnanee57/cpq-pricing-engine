import datetime
import json
import math
import os
import traceback
import zlib

import pandas as pd
import ray
from sqlalchemy import text

from configure.config import app
from forecast.forecast import is_float
from forecast.forecast_views import get_mprn_forecast
from models.models import PricingData, db, PricingSegmentGroupEvaluationModelEO, \
    PricingSegmentGroupEvaluationModelVersionEO
from services.common.encoder import NpJsonEncoder
from services.common.timer import Timer
from services.services import sendPricingData, send_price_status, send_for_hold_price_evaluation


def get_eval_status(price_status_set):
    if 'FAILED' in price_status_set:
        if 'WARNING' in price_status_set:
            eval_status = 'FAILED_WARNING'
        else:
            eval_status = 'FAILED'
    elif 'WARNING' in price_status_set:
        eval_status = 'WARNING'
    else:
        eval_status = 'SUCCESS'
    return eval_status


def get_day_data_obj():
    day_data = {'Date': ''}
    for i in range(1, 37):
        day_data['c' + str(i) + '_rolling'] = 0.0
    for i in range(1, 37):
        day_data['c' + str(i) + '_customer'] = 0.0
    day_data['Rolling AQ'] = 0.0
    day_data['Customer AQ'] = 0.0
    return day_data


def cal_days_diff(a, b):
    a_ = a.replace(hour=0, minute=0, second=0, microsecond=0)
    b_ = b.replace(hour=0, minute=0, second=0, microsecond=0)
    return (a_ - b_).days + 1


def cal_days_diff_acc(a, b):
    a_ = a.replace(hour=0, minute=0, second=0, microsecond=0)
    b_ = b.replace(hour=0, minute=0, second=0, microsecond=0)
    return (a_ - b_).days


def cost_calc_type1(zco_1, zco_2, zco_3, totalAq, aq, zco_exp, zco_min, formula_soq):
    if 1 <= totalAq <= 73200:
        return zco_1 * aq
    elif 73201 <= totalAq <= 732000:
        return zco_2 * aq
    elif totalAq >= 732001:
        if zco_exp is None or zco_exp == '':
            return zco_3 * aq
        else:
            return max(zco_min, zco_3 * (formula_soq ** zco_exp)) * aq
    else:
        return 0


def cost_calc_type3(zco_1, zco_2, zco_3, totalAq, aq, zco_exp, zco_min, formula_soq):
    if 1 <= totalAq <= 73200:
        return zco_1 * formula_soq
    elif 73201 <= totalAq <= 732000:
        return zco_2 * formula_soq
    elif totalAq >= 732001:
        if zco_exp is None or zco_exp == '':
            return zco_3 * aq
        else:
            return max(zco_min, zco_3 * (formula_soq ** zco_exp)) * formula_soq
    else:
        return 0


def cost_calc_type2(totalAq, val):
    if 73201 <= totalAq <= 732000 and val is not None:
        return val
    else:
        return 0


def get_uni_gas_keys(uni_gas_ranges, class_, aq):

    uni_gas_key = None

    for row in range(len(uni_gas_ranges)):
        low_range = uni_gas_ranges[row][0]
        high_range = uni_gas_ranges[row][1]
        key_string = 'Uni_Cost_' + str(class_) + '_' + str(low_range) + ('_nan' if math.isnan(high_range)
                                                                         else '_' + str(high_range))

        if low_range <= aq and (math.isnan(high_range) or aq <= high_range):
            uni_gas_key = key_string
        # if customer_aq_sum_cond and low_range <= customer_aq_sum and (math.isnan(high_range) or
        #                                                               customer_aq_sum <= high_range):
        #     uni_gas_cust_key = key_string

    return uni_gas_key


def create_price_evaluation(pricing_dict, isHold, quote_period_days,
                            holdRollingUnitRate, holdRollingStandingCharge,
                            holdCustomerUnitRate, holdCustomerStandingCharge, cobd_val, copt_dict,
                            payment_period, free_days, processing_days, processing_cost, day_wise_data_type):
    try:
        pricing_df = pd.DataFrame(pricing_dict)
        total_sum = pricing_df.sum().tolist()
        pricing_df['Date'] = pd.to_datetime(pricing_df['Date'], format='%Y-%m-%d')
        rolling_sum, customer_sum = total_sum[1:37], total_sum[37:73]
        rolling_sum.insert(0, total_sum[-2])
        customer_sum.insert(0, total_sum[-1])

        rolling_sum_vol_cond = not ((rolling_sum[0] != '' and math.isnan(rolling_sum[0])) or rolling_sum[0] == '')
        customer_sum_vol_cond = not ((customer_sum[0] != '' and math.isnan(customer_sum[0])) or customer_sum[0] == '')

        rolling_sum[0] = rolling_sum[0] if rolling_sum_vol_cond else 0
        customer_sum[0] = customer_sum[0] if customer_sum_vol_cond else 0

        fal_roll_sum = 0
        fal_cust_sum = 0

        if isHold:
            fal_roll_sum += (holdRollingUnitRate * rolling_sum[0]) + (holdRollingStandingCharge * quote_period_days)
            fal_cust_sum += (holdCustomerUnitRate * customer_sum[0] + holdCustomerStandingCharge * quote_period_days)
            for i in range(30, 36):
                i_roll_sum = rolling_sum[i]
                i_cust_sum = customer_sum[i]
                fal_roll_sum -= 0 if (i_roll_sum != '' and math.isnan(i_roll_sum)) or i_roll_sum == '' else i_roll_sum
                fal_cust_sum -= 0 if (i_cust_sum != '' and math.isnan(i_cust_sum)) or i_cust_sum == '' else i_cust_sum
        else:
            for i in range(1, 28):
                i_roll_sum = rolling_sum[i]
                i_cust_sum = customer_sum[i]
                fal_roll_sum += 0 if (i_roll_sum != '' and math.isnan(i_roll_sum)) or i_roll_sum == '' else i_roll_sum
                fal_cust_sum += 0 if (i_cust_sum != '' and math.isnan(i_cust_sum)) or i_cust_sum == '' else i_cust_sum
            i_roll_sum = rolling_sum[36]
            i_cust_sum = customer_sum[36]
            fal_roll_sum += 0 if (i_roll_sum != '' and math.isnan(i_roll_sum)) or i_roll_sum == '' else i_roll_sum
            fal_cust_sum += 0 if (i_cust_sum != '' and math.isnan(i_cust_sum)) or i_cust_sum == '' else i_cust_sum

        for i in range(len(copt_dict)):
            copt = copt_dict[i]['Value']
            copt_val = ((((payment_period - free_days + processing_days) /
                          quote_period_days) * (copt / 100)) + processing_cost)
            date = copt_dict[i]['Effective_Month']
            month = date.month
            year = date.year
            fal_roll_sum_ = ((fal_roll_sum * 100) / (100 - cobd_val + copt_val)) / quote_period_days
            fal_cust_sum_ = ((fal_cust_sum * 100) / (100 - cobd_val + copt_val)) / quote_period_days

            cobd_rolling = fal_roll_sum_ * cobd_val / 100
            cobd_cust = fal_cust_sum_ * cobd_val / 100
            copt_rolling = fal_roll_sum_ * copt_val / 100
            copt_cust = fal_cust_sum_ * copt_val / 100

            pricing_df.loc[(pricing_df['Date'].dt.month == month) &
                           (pricing_df['Date'].dt.year == year), ['c28_rolling', 'c28_customer',
                                                                  'c29_rolling', 'c29_customer']] = \
                [cobd_rolling, cobd_cust, copt_rolling, copt_cust]

        rolling_sum[28] = pricing_df['c28_rolling'].sum()
        rolling_sum[29] = pricing_df['c29_rolling'].sum()
        customer_sum[28] = pricing_df['c28_customer'].sum()
        customer_sum[29] = pricing_df['c29_customer'].sum()

        if day_wise_data_type == 'MONTH_WISE':
            pricing_df = pricing_df.resample('M', on='Date').sum()
            pricing_df = pricing_df.reset_index()
            pricing_df['Date'] = pricing_df['Date'].dt.strftime('%Y-%m')
        elif day_wise_data_type == 'YEAR_WISE':
            pricing_df = pricing_df.resample('Y', on='Date').sum()
            pricing_df = pricing_df.reset_index()
            pricing_df['Date'] = pricing_df['Date'].dt.strftime('%Y')
        else:
            pricing_df['Date'] = pricing_df['Date'].dt.strftime('%Y-%m-%d')

        unit_cal_index = {1, 2, 4, 12, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25, 28, 29, 30, 32, 34,36}
        standing_index = {3, 6, 7, 9, 10, 11, 13, 15, 27, 31, 33, 35}

        rolling_unit_cost = 0
        customer_unit_cost = 0
        rolling_standing_cost = 0
        customer_standing_cost = 0

        for i in unit_cal_index:
            i_roll_sum = rolling_sum[i]
            i_cust_sum = customer_sum[i]
            rolling_unit_cost += 0 if (i_roll_sum != '' and math.isnan(i_roll_sum)) or i_roll_sum == '' else i_roll_sum
            customer_unit_cost += 0 if (i_cust_sum != '' and math.isnan(i_cust_sum)) or i_cust_sum == '' else i_cust_sum
        for i in standing_index:
            i_roll_sum = rolling_sum[i]
            i_cust_sum = customer_sum[i]
            rolling_standing_cost += 0 if (i_roll_sum != '' and math.isnan(i_roll_sum)) or i_roll_sum == '' else \
                i_roll_sum
            customer_standing_cost += 0 if (i_cust_sum != '' and math.isnan(i_cust_sum)) or i_cust_sum == '' else \
                i_cust_sum

        if isHold:
            # rolling
            rolling_unit_rate = holdRollingUnitRate
            rolling_standing_charge = holdRollingStandingCharge
            rolling_unit_revenue = holdRollingUnitRate * rolling_sum[0]
            rolling_standing_revenue = holdRollingStandingCharge * quote_period_days
            rolling_unit_rate_margin = rolling_unit_revenue - rolling_unit_cost
            rolling_standing_charge_margin = rolling_standing_revenue - rolling_standing_cost
            rolling_sum[26] = rolling_unit_rate_margin + rolling_standing_charge_margin
            try:
                rolling_unit_rate_margin_per_kwh = 0 if rolling_sum[0] == 0 else (rolling_unit_rate_margin / rolling_sum[0])
            except ZeroDivisionError:
                rolling_unit_rate_margin_per_kwh = 0

            # customer
            customer_unit_rate = holdCustomerUnitRate
            customer_standing_charge = holdCustomerStandingCharge
            customer_unit_revenue = holdCustomerUnitRate * customer_sum[0]
            customer_standing_revenue = holdCustomerStandingCharge * quote_period_days
            customer_unit_rate_margin = customer_unit_revenue - customer_unit_cost
            customer_standing_charge_margin = customer_standing_revenue - customer_standing_cost
            customer_sum[26] = customer_unit_rate_margin + customer_standing_charge_margin
            try:
                customer_unit_rate_margin_per_kwh = 0 if customer_sum[0] == 0 else (customer_unit_rate_margin / customer_sum[0])
            except ZeroDivisionError:
                customer_unit_rate_margin_per_kwh = 0
        else:
            # rolling
            rolling_margin = rolling_sum[26]
            rolling_unit_revenue = rolling_unit_cost + (0 if (
                    (rolling_margin != '' and math.isnan(rolling_margin)) or rolling_margin == '') else rolling_margin)
            rolling_standing_revenue = rolling_standing_cost
            rolling_unit_rate = 0 if rolling_sum[0] == 0 else (rolling_unit_revenue / rolling_sum[0])
            rolling_standing_charge = rolling_standing_revenue / quote_period_days
            rolling_unit_rate_margin = rolling_unit_revenue - rolling_unit_cost
            try:
                rolling_unit_rate_margin_per_kwh = 0 if rolling_sum[0] == 0 else (rolling_unit_rate_margin / rolling_sum[0])
            except ZeroDivisionError:
                rolling_unit_rate_margin_per_kwh = 0
            rolling_standing_charge_margin = rolling_standing_revenue - rolling_standing_cost

            # customer
            cust_margin = customer_sum[26]
            customer_unit_revenue = customer_unit_cost + (0 if (
                    (cust_margin != '' and math.isnan(cust_margin)) or cust_margin == '') else cust_margin)
            customer_standing_revenue = customer_standing_cost
            customer_unit_rate = 0 if customer_sum[0] == 0 else (customer_unit_revenue / customer_sum[0])
            customer_standing_charge = customer_standing_revenue / quote_period_days
            customer_unit_rate_margin = customer_unit_revenue - customer_unit_cost
            try:
                customer_unit_rate_margin_per_kwh = 0 if customer_sum[0] == 0 else (customer_unit_rate_margin / customer_sum[0])
            except ZeroDivisionError:
                customer_unit_rate_margin_per_kwh = 0
            customer_standing_charge_margin = customer_standing_revenue - customer_standing_cost

        rolling_total_cost = rolling_unit_cost + rolling_standing_cost
        rolling_total_revenue = rolling_unit_revenue + rolling_standing_revenue
        rolling_total_margin = rolling_unit_rate_margin + rolling_standing_charge_margin

        customer_total_cost = customer_unit_cost + customer_standing_cost
        customer_total_revenue = customer_unit_revenue + customer_standing_revenue
        customer_total_margin = customer_unit_rate_margin + customer_standing_charge_margin

        rolling_eval_dict = {"C" + str(i): rolling_sum[i] for i in range(1, len(rolling_sum))}
        rolling_eval_dict["Forecast Volume"] = rolling_sum[0]
        rolling_eval_dict["Quote Period"] = quote_period_days
        rolling_eval_dict["UnitRate-Cost"] = rolling_unit_cost
        rolling_eval_dict["UnitRate-Revenue"] = rolling_unit_revenue
        rolling_eval_dict["Unit Rate"] = rolling_unit_rate
        rolling_eval_dict["UnitRate-Margin"] = rolling_unit_rate_margin
        rolling_eval_dict["Unit-Margin-per-kwh"] = rolling_unit_rate_margin_per_kwh
        rolling_eval_dict["StandingCharge-Cost"] = rolling_standing_cost
        rolling_eval_dict["StandingCharge-Revenue"] = rolling_standing_revenue
        rolling_eval_dict["Standing Charge"] = rolling_standing_charge
        rolling_eval_dict["StandingCharge-Margin"] = rolling_standing_charge_margin
        rolling_eval_dict["Total Cost"] = rolling_total_cost
        rolling_eval_dict["Total Revenue"] = rolling_total_revenue
        rolling_eval_dict["Total Margin"] = rolling_total_margin

        customer_eval_dict = {"C" + str(i): customer_sum[i] for i in range(1, len(customer_sum))}
        customer_eval_dict["Forecast Volume"] = customer_sum[0]
        customer_eval_dict["Quote Period"] = quote_period_days
        customer_eval_dict["UnitRate-Cost"] = customer_unit_cost
        customer_eval_dict["UnitRate-Revenue"] = customer_unit_revenue
        customer_eval_dict["Unit Rate"] = customer_unit_rate
        customer_eval_dict["UnitRate-Margin"] = customer_unit_rate_margin
        customer_eval_dict["Unit-Margin-per-kwh"] = customer_unit_rate_margin_per_kwh
        customer_eval_dict["StandingCharge-Cost"] = customer_standing_cost
        customer_eval_dict["StandingCharge-Revenue"] = customer_standing_revenue
        customer_eval_dict["Standing Charge"] = customer_standing_charge
        customer_eval_dict["StandingCharge-Margin"] = customer_standing_charge_margin
        customer_eval_dict["Total Cost"] = customer_total_cost
        customer_eval_dict["Total Revenue"] = customer_total_revenue
        customer_eval_dict["Total Margin"] = customer_total_margin

        return [rolling_eval_dict, customer_eval_dict], pricing_df.to_json(orient="records"), "", True
    except Exception as err:
        print(traceback.format_exc())
        return [{}, {}], "", err, False


def get_cts_keys(unique_mprn_count, cts_aq_ranges, cts_mprn_ranges, hh_indicator, product_indicator,
                 gain_retain, third_party_indicator, rolling_aq_sum_cond, rolling_aq_sum,
                 customer_aq_sum_cond, customer_aq_sum):

    cts_rolling_key, cts_cust_key = None, None

    cts_count = 0
    for j in cts_aq_ranges:
        low_aq_range = j[0]
        high_aq_range = j[1]
        if cts_count == 2:
            break
        if rolling_aq_sum_cond and low_aq_range <= rolling_aq_sum and (
                math.isnan(high_aq_range) or rolling_aq_sum <= high_aq_range):
            for k in cts_mprn_ranges:
                low_mprn_range = k[0]
                high_mprn_range = k[1]
                cts_key_string = hh_indicator + '_' + product_indicator + '_' + gain_retain + '_' + \
                    third_party_indicator + ('' if low_aq_range == 0 else '_' + str(int(low_aq_range))) + \
                    '_' + str(high_aq_range) + ('' if low_mprn_range == 0 else '_' + str(int(low_mprn_range))) \
                    + '_' + str(high_mprn_range)
                if low_mprn_range <= unique_mprn_count and \
                        (math.isnan(high_mprn_range) or unique_mprn_count <= high_mprn_range):
                    cts_rolling_key = cts_key_string
                    cts_count += 1
                    break
        if customer_aq_sum_cond and low_aq_range <= customer_aq_sum and (
                math.isnan(high_aq_range) or customer_aq_sum <= high_aq_range):
            for k in cts_mprn_ranges:
                low_mprn_range = k[0]
                high_mprn_range = k[1]
                cts_key_string = hh_indicator + '_' + product_indicator + '_' + gain_retain + '_' + \
                    third_party_indicator + ('' if low_aq_range == 0 else '_' + str(int(low_aq_range))) + \
                    '_' + str(high_aq_range) + ('' if low_mprn_range == 0 else '_' + str(int(low_mprn_range))) \
                    + '_' + str(high_mprn_range)
                if low_mprn_range <= unique_mprn_count and \
                        (math.isnan(high_mprn_range) or unique_mprn_count <= high_mprn_range):
                    cts_cust_key = cts_key_string
                    cts_count += 1
                    break
    return cts_rolling_key, cts_cust_key


def get_cobd_key(cobd_uplift_flag, cobd_cra_ranges, cobd_pb_ranges, cv_score, gain_retain, payment_behaviour,
                 dd_indicator, credit_rating, bill_freq):

    key_string = ''
    for j in cobd_cra_ranges:
        low_cra_range = j[0]
        high_cra_range = j[1]
        if not math.isnan(credit_rating) and float(low_cra_range) <= credit_rating <= float(high_cra_range):
            for k in cobd_pb_ranges:
                low_pb_range = int(k[0]) if k[0] != '' else math.nan
                high_pb_range = int(k[1]) if k[1] != '' else math.nan
                low_pb_cond = math.isnan(low_pb_range)
                high_pb_cond = math.isnan(high_pb_range)
                input_pb_cond = math.isnan(payment_behaviour)

                null_case = low_pb_cond and high_pb_cond and input_pb_cond
                other_case = (not (math.isnan(low_pb_range) and math.isnan(high_pb_range))) and \
                             (math.isnan(low_pb_range) or low_pb_range <= payment_behaviour) and \
                             (math.isnan(high_pb_range) or payment_behaviour <= high_pb_range)

                if null_case or other_case:
                    key_string = 'Uplifted_Value_' if cobd_uplift_flag == 'Y' else 'Value_'
                    key_string += (str(cv_score) if isinstance(cv_score, int) else 'Blank') + '_'
                    key_string += str(low_cra_range) + '_' + str(high_cra_range) + '_'
                    key_string += '' if math.isnan(low_pb_range) else (str(low_pb_range) + '_')
                    key_string += '' if math.isnan(high_pb_range) else (str(high_pb_range) + '_')
                    key_string += 'E' if gain_retain == 'R' else 'N'
                    key_string += '_' + dd_indicator + '_' + bill_freq
                    break
            break
    return key_string


def get_timing_risk(day_diff, timing_risk_data):
    timing_risk_val = 0
    for j in timing_risk_data:
        if j[0] <= day_diff <= j[1]:
            timing_risk_val = j[2]
            break
    return timing_risk_val


def cost_calc_transport(cost_ranges, unit, formula_aq_conv, formula_soq, zco_val_dict):
    rate = 0
    for i in cost_ranges:
        min_aq = i[0]
        max_aq = i[1] if i[1] != '' else math.nan
        max_aq_cond = math.isnan(max_aq)
        if min_aq <= formula_aq_conv and (max_aq_cond or (not max_aq_cond and formula_aq_conv <= max_aq)):
            if not max_aq_cond:
                rate = float(zco_val_dict['Transport_Cost_' + str(min_aq) + '_' + str(max_aq) + '_' + unit])
            else:
                val = float(zco_val_dict['Transport_Cost_' + str(min_aq) + '_' + unit])
                exp = float(zco_val_dict['Transport_Cost_' + str(min_aq) + '_Exponent Value'])
                min = float(zco_val_dict['Transport_Cost_' + str(min_aq) + '_Minimum charge'])
                if exp is None or exp == '':
                    rate = val
                else:
                    rate = max(min, val * (formula_soq ** exp))
            break
    return rate


def cost_calc_transport2(ranges, totalAq, val):
    for i in ranges:
        min_aq = i[0] if i[0] != '' else math.nan
        max_aq = i[1] if i[1] != '' else math.nan
        min_aq_cond = math.isnan(min_aq)
        max_aq_cond = math.isnan(max_aq)
        cond1 = (min_aq_cond or (not min_aq_cond and min_aq <= totalAq))
        cond2 = (max_aq_cond or (not max_aq_cond and totalAq <= max_aq))

        if cond1 and cond2:
            return val if val is not None else 0
        else:
            return 0


def calculate_pricing(mprnData, unique_mprn_count, quote_period_days, rolling_aq_sum, customer_aq_sum,
                      pricing_dict, price_curve_data_dict, gnt_dict, ecn_dict, zco_dict, zca_dict, cca_dict, cfi_dict,
                      solr_dict, uni_gas_dict, transport_ranges, uni_gas_ranges, ggl_fixed_dict, ggl_variable_dict,
                      xo_admin_dict, timing_risk_data, hedging_cost_dict, timing_risk_monthly_dict, hedging_risk_dict,
                      imb_cost_dict,
                      imb_risk_dict, risk_premium_dict, energy_balance_dict, gas_trading_dict, cts_dict, cts_aq_ranges,
                      cts_mprn_ranges, cobd_cra_ranges, cobd_pb_ranges, cobd_dict, copt_dict, primary_tpi_dict,
                      secondary_tpi_dict, tertiary_tpi_dict, failRemarks):
    processing_days_dict = {
        "BACS": 3,
        "CASH": 1,
        "CHAPS": 3,
        "CHEQUE": 6,
        "DD": 0,
        "QUARTERLY DD": -25,
    }

    processing_cost_dict = {
        "BACS": 0.06,
        "CASH": 0.06,
        "CHAPS": 0.06,
        "CHEQUE": 0.06,
        "DD": 0.00,
        "QUARTERLY DD": 0.06,
    }

    try:
        input_remarks = []
        day_wise_data_type = mprnData['dayWiseData']
        is_hold = True if mprnData['isHoldPrice'] == 'Y' else False
        timing_risk_val_list = []
        timing_risk_calc_flag = False

        try:
            formula_soq = float(mprnData['FormulaSOQ'])
        except ValueError:
            input_remarks += ['E_FSOQ_D']

        try:
            customer_soq = float(mprnData['CustomerSOQ'])
        except ValueError:
            input_remarks += ['E_CSOQ_D']

        rolling_aq = mprnData['RollingAQ']
        customer_aq = mprnData['CustomerAQ']
        formula_aq = mprnData['FormulaAQ']
        aq_used = mprnData['AQ_USED']

        margin = str(mprnData['Margin'])
        if margin != '' and is_float(margin):
            margin = float(margin)
        elif margin == '':
            margin = 0.0
        else:
            input_remarks += ['E_MARGIN_D']

        try:
            hh_indicator = mprnData['HH_Indicator']
        except ValueError:
            input_remarks += ['E_HHI_D']

        try:
            product_indicator = mprnData['ProductType']
        except ValueError:
            input_remarks += ['E_PI_D']

        cv_score = mprnData['CV_Score']
        if cv_score != '' and (isinstance(cv_score, str) or not math.isnan(cv_score)):
            try:
                cv_score = int(mprnData['CV_Score'][2:])
            except (KeyError, IndexError, ValueError):
                input_remarks += ['E_CV_D']

        gain_retain = mprnData['GainRetain']
        if not (gain_retain == 'G' or gain_retain == 'R'):
            input_remarks += ['E_GR_D']

        new_existing = mprnData['NewExisting']
        if not (new_existing == 'E' or new_existing == 'N'):
            input_remarks += ['E_NEWEX_D']

        third_party_indicator = mprnData['ThirdPartyIndicator']
        if not (third_party_indicator == 'Y' or third_party_indicator == 'N'):
            input_remarks += ['E_TPII_D']

        market_sector_code = mprnData['MarketSectorCode']
        if market_sector_code != 'I' and market_sector_code != "D":
            input_remarks += ['E_MKC_D']
            
        meter_read_batch_freq = mprnData["MeterReadBatchFreq"]

        try:
            class_ = int(mprnData['Class'])
        except ValueError:
            input_remarks += ['E_CLASS_D']

        try:
            credit_rating = float(mprnData['CreditRating'])
        except ValueError:
            input_remarks += ['E_CRA_D']

        try:
            payment_behaviour = float(mprnData['PaymentBehaviour'])
        except ValueError:
            input_remarks += ['E_PYB_D']

        try:
            payment_period = float(mprnData['PaymentPeriod'])
        except ValueError:
            input_remarks += ['E_PYP_D']

        try:
            payment_method = mprnData['PaymentMethod']
            processing_days = processing_days_dict[payment_method]
            processing_cost = processing_cost_dict[payment_method]
        except KeyError:
            input_remarks += ['E_PYM_D']

        try:
            free_days = int(mprnData['FreeDay'])
        except ValueError:
            input_remarks += ['E_FDAYS_D']

        dd_indicator = mprnData['DD_Indicator']
        if not (dd_indicator == 'DD' or dd_indicator == 'NDD'):
            input_remarks += ['E_DDI_D']

        bill_freq = mprnData['BillCycle']
        if not (bill_freq == 'M' or bill_freq == 'Q'):
            input_remarks += ['E_BFREQ_D']

        cobd_uplift_flag = mprnData['CobtUpliftFlag']

        try:
            p_comm_unit_rate = 0 if mprnData['Commission_Value_Unit_Primary'] == '' else float(
                mprnData['Commission_Value_Unit_Primary'])
            p_comm_standing_charge = 0 if mprnData['Commission_Value_Standing_Primary'] == '' else float(
                mprnData['Commission_Value_Standing_Primary'])
            s_comm_unit_rate = 0 if mprnData['Commission_Value_Unit_Secondary'] == '' else float(
                mprnData['Commission_Value_Unit_Secondary'])
            s_comm_standing_charge = 0 if mprnData['Commission_Value_Standing_Secondary'] == '' else float(
                mprnData['Commission_Value_Standing_Secondary'])
            t_comm_unit_rate = 0 if mprnData['Commission_Value_Unit_Tertiary'] == '' else float(
                mprnData['Commission_Value_Unit_Tertiary'])
            t_comm_standing_charge = 0 if mprnData['Commission_Value_Standing_Tertiary'] == '' else float(
                mprnData['Commission_Value_Standing_Tertiary'])
        except ValueError:
            input_remarks += ['E_TPI_D']

        if is_hold:
            try:
                holdRollingUnitRate = float(mprnData['holdRollingUnitRate'])
                holdRollingStandingCharge = float(mprnData['holdRollingStandingCharge'])
                holdCustomerUnitRate = float(mprnData['holdCustomerUnitRate'])
                holdCustomerStandingCharge = float(mprnData['holdCustomerStandingCharge'])
            except ValueError:
                input_remarks += ['E_HOLD_D']
        else:
            holdRollingUnitRate = 0
            holdRollingStandingCharge = 0
            holdCustomerUnitRate = 0
            holdCustomerStandingCharge = 0

        rolling_aq_cond = is_float(str(rolling_aq))
        customer_aq_cond = is_float(str(customer_aq))
        formula_aq_cond = is_float(str(formula_aq))

        if aq_used == 'Rolling AQ' and (rolling_aq == '' or not rolling_aq_cond):
            input_remarks += ['E_RAQ_D']

        if aq_used == 'Customer AQ' and (customer_aq == '' or not customer_aq_cond):
            input_remarks += ['E_CAQ_D']

        if formula_aq == '' or not formula_aq_cond:
            input_remarks += ['E_FAQ_D']

        # try:
        #     float(cfi_dict[0]["Transport_Cost_CFI " + meter_read_batch_freq])
        # except (KeyError, ValueError):
        #     input_remarks += ['E_CFI_D']
        #
        # try:
        #     float(solr_dict[0]['Transport_Cost_' + market_sector_code])
        # except (KeyError, ValueError):
        #     input_remarks += ['E_SOLR_D']

        if len(input_remarks) == 0:

            formula_aq_conv = float(formula_aq)

            fail_remarks = failRemarks
            pricing_warnings = []

            rolling_aq_sum_cond = is_float(str(rolling_aq_sum))
            customer_aq_sum_cond = is_float(str(customer_aq_sum))

            uni_gas_key = get_uni_gas_keys(uni_gas_ranges, class_, formula_aq_conv)

            cts_rolling_key, cts_cust_key = get_cts_keys(unique_mprn_count, cts_aq_ranges, cts_mprn_ranges,
                                                         hh_indicator, product_indicator, gain_retain,
                                                         third_party_indicator, rolling_aq_sum_cond, rolling_aq_sum,
                                                         customer_aq_sum_cond, customer_aq_sum)

            try:
                cts_unit_rolling = cts_dict[0]['CTS_UNIT' + '_' + cts_rolling_key]
                cts_unit_cust = cts_dict[0]['CTS_UNIT' + '_' + cts_cust_key]
                cts_standing_rolling = cts_dict[0]['CTS_STANDING' + '_' + cts_rolling_key]
                cts_standing_cust = cts_dict[0]['CTS_STANDING' + '_' + cts_cust_key]
                calc_cts = True
            except (KeyError, TypeError):
                if not (rolling_aq_cond or customer_aq_cond):
                    fail_remarks += ['E_CTS_D']
                else:
                    calc_cts = False

            cobd_val = 0.0

            cobd_key = get_cobd_key(cobd_uplift_flag, cobd_cra_ranges, cobd_pb_ranges, cv_score, new_existing,
                                    payment_behaviour, dd_indicator, credit_rating, bill_freq)

            try:
                cobd_val = cobd_dict[0][cobd_key]
            except KeyError:
                print("Cobd_Key: " + cobd_key + ' Not Found.')
                fail_remarks += ['E_COBD_D']

            if third_party_indicator == 'Y':
                if not (math.isnan(p_comm_unit_rate) and math.isnan(p_comm_standing_charge)) \
                        and len(primary_tpi_dict) > 0:
                    if not math.isnan(p_comm_unit_rate) and math.isnan(p_comm_standing_charge):
                        if p_comm_unit_rate > primary_tpi_dict[0]['UNIT_Value']:
                            pricing_warnings += ['E_LIMITP_U']
                    else:
                        if p_comm_standing_charge > primary_tpi_dict[0]['FIXED_Value']:
                            pricing_warnings += ['E_LIMITP_S']
                if not (math.isnan(s_comm_unit_rate) and math.isnan(s_comm_standing_charge)) and len(
                        secondary_tpi_dict) > 0:
                    if not math.isnan(s_comm_unit_rate) and math.isnan(s_comm_standing_charge):
                        if s_comm_unit_rate > secondary_tpi_dict[0]['UNIT_Value']:
                            pricing_warnings += ['E_LIMITS_U']
                    else:
                        if s_comm_standing_charge > secondary_tpi_dict[0]['FIXED_Value']:
                            pricing_warnings += ['E_LIMITS_S']
                if not (math.isnan(t_comm_unit_rate) and math.isnan(t_comm_standing_charge)) \
                        and len(tertiary_tpi_dict) > 0:
                    if not math.isnan(t_comm_unit_rate) and math.isnan(t_comm_standing_charge):
                        if t_comm_unit_rate > tertiary_tpi_dict[0]['UNIT_Value']:
                            pricing_warnings += ['E_LIMITT_U']
                    else:
                        if t_comm_standing_charge > tertiary_tpi_dict[0]['FIXED_Value']:
                            pricing_warnings += ['E_LIMITT_S']

            if len(fail_remarks) == 0:
                for i in range(len(pricing_dict)):
                    pricing_day_data = pricing_dict[i]

                    date = pricing_day_data['Date']
                    day_rolling_aq = pricing_day_data['Rolling AQ']
                    day_customer_aq = pricing_day_data['Customer AQ']

                    day_data = get_day_data_obj()
                    day_data['Date'] = date.strftime('%Y-%m-%d')
                    day_data['Rolling AQ'] = day_rolling_aq
                    day_data['Customer AQ'] = day_customer_aq

                    rolling_alp_rate = round(float(day_rolling_aq), 4) if day_rolling_aq != '' else math.nan
                    customer_alp_rate = round(float(day_customer_aq), 4) if day_customer_aq != '' else math.nan

                    try:
                        price_curve_value = float(price_curve_data_dict[i]['Charge(p/therm)']) / 29.3071
                    except (KeyError, ValueError):
                        try:
                            price_curve_value = float(price_curve_data_dict[i]['Charge (p/therm)']) / 29.3071
                        except (KeyError, ValueError):
                            price_curve_value = None
                            fail_remarks += ['E_PRICE_D']

                    price_curve_cond = price_curve_value is not None
                    rolling_alp_rate_cond = not math.isnan(rolling_alp_rate)
                    customer_alp_rate_cond = not math.isnan(customer_alp_rate)

                    if price_curve_cond:
                        if formula_aq >= 1:
                            gnt_cost = float(gnt_dict[i]['Transport_Cost'])
                            ecn_cost = float(ecn_dict[i]['Transport_Cost'])
                            try:
                                cfi_rate = float(cfi_dict[i]["Transport_Cost_CFI " + meter_read_batch_freq])
                            except (KeyError, ValueError):
                                fail_remarks += ['E_CFI_D']

                            try:
                                solr_rate = float(solr_dict[i]['Transport_Cost_' + market_sector_code])
                            except (KeyError, ValueError):
                                fail_remarks += ['E_SOLR_D']
                        else:
                            gnt_cost = 0
                            ecn_cost = 0
                            cfi_rate = 0
                            solr_rate = 0

                        # zco_cost_1st_range = float(zco_dict[i]['Transport_Cost_1_73200_p/kWh'])
                        # zco_cost_2nd_range = float(zco_dict[i]['Transport_Cost_73201_732000_p/kWh'])
                        # zco_cost_3rd_range = float(zco_dict[i]['Transport_Cost_732001_p/kWh'])
                        # zco_exponent = float(zco_dict[i]['Transport_Cost_732001_Exponent Value'])
                        # zco_min = float(zco_dict[i]['Transport_Cost_732001_Minimum charge'])
                        # zca_cost_1st_range = float(zca_dict[i]['Transport_Cost_1_73200_pence/peak day kWh/day'])
                        # zca_cost_2nd_range = float(zca_dict[i]['Transport_Cost_73201_732000_pence/peak day kWh/day'])
                        # zca_cost_3rd_range = float(zca_dict[i]['Transport_Cost_732001_pence/peak day kWh/day'])
                        # zca_exponent = float(zca_dict[i]['Transport_Cost_732001_Exponent Value'])
                        # zca_min = float(zca_dict[i]['Transport_Cost_732001_Minimum charge'])
                        # cca_cost_1st_range = float(cca_dict[i]['Transport_Cost_1_73200_pence/peak day kWh/day'])
                        # cca_cost_2nd_range = float(cca_dict[i]['Transport_Cost_73201_732000_pence/peak day kWh/day'])
                        # cca_cost_3rd_range = float(cca_dict[i]['Transport_Cost_732001_pence/peak day kWh/day'])
                        # cca_exponent = float(cca_dict[i]['Transport_Cost_732001_Exponent Value'])
                        # cca_min = float(cca_dict[i]['Transport_Cost_732001_Minimum charge'])

                        try:
                            uni_gas_val = float(uni_gas_dict[i][uni_gas_key])
                        except (KeyError, ValueError):
                            fail_remarks += ['E_UNIGAS_D']

                        ggl_fixed = ggl_fixed_dict[i]["Charge"]
                        ggl_variable = ggl_variable_dict[i]["Charge"]
                        xo_admin = xo_admin_dict[i]["Charge"]
                        hedging_cost = hedging_cost_dict[i]["Charge"]
                        hedging_risk = hedging_risk_dict[i]["Charge"]
                        timing_risk_monthly = timing_risk_monthly_dict[i]["Charge"]
                        imb_cost = imb_cost_dict[i]["Charge"]
                        imb_risk = imb_risk_dict[i]["Charge"]
                        risk_premium = risk_premium_dict[i]["Charge"]
                        energy_bal = energy_balance_dict[i]["Charge"]
                        gts_val = gas_trading_dict[i]['Charge']

                        if not timing_risk_calc_flag:
                            timing_risk_val = get_timing_risk(cal_days_diff_acc(date, datetime.datetime.now()),
                                                              timing_risk_data)
                            timing_risk_val_list += [timing_risk_val]

                        cfi_cost = cost_calc_transport2(transport_ranges["CFI"], formula_aq_conv, cfi_rate)

                        if rolling_alp_rate_cond:
                            # values dependent on rolling aq
                            if len(fail_remarks) == 0:
                                c1_rolling = rolling_alp_rate * price_curve_value
                                day_data['c1_rolling'] = c1_rolling
                                day_data['c2_rolling'] = rolling_alp_rate * gnt_cost
                                day_data['c3_rolling'] = ecn_cost * formula_soq

                                zco_rate = cost_calc_transport(transport_ranges["ZCO"], 'p/kWh',
                                                               formula_aq_conv,
                                                               formula_soq, zco_dict[i])
                                zca_cost = cost_calc_transport(transport_ranges["ZCA"],
                                                               'pence/peak day kWh/day',
                                                               formula_aq_conv, formula_soq, zca_dict[i]) * formula_soq
                                cca_cost = cost_calc_transport(transport_ranges["CCA"],
                                                               'pence/peak day kWh/day',
                                                               formula_aq_conv, formula_soq, cca_dict[i]) * formula_soq

                                # day_data['c4_rolling'] = cost_calc_type1(zco_cost_1st_range, zco_cost_2nd_range,
                                #                                          zco_cost_3rd_range, formula_aq_conv,
                                #                                          rolling_alp_rate, zco_exponent, zco_min,
                                #                                          formula_soq)
                                # day_data['c6_rolling'] = \
                                #     cost_calc_type3(zca_cost_1st_range, zca_cost_2nd_range, zca_cost_3rd_range,
                                #                     formula_aq_conv, rolling_alp_rate, zca_exponent, zca_min,
                                #                     formula_soq)
                                # day_data['c7_rolling'] = \
                                #     cost_calc_type3(cca_cost_1st_range, cca_cost_2nd_range, cca_cost_3rd_range,
                                #                     formula_aq_conv, rolling_alp_rate, cca_exponent, cca_min,
                                #                     formula_soq)
                                #
                                # day_data['c9_rolling'] = cost_calc_type2(formula_aq_conv, cfi_rate)

                                day_data['c4_rolling'] = zco_rate * rolling_alp_rate
                                day_data['c6_rolling'] = zca_cost
                                day_data['c7_rolling'] = cca_cost
                                day_data['c9_rolling'] = cfi_cost

                                solr_val_rolling = 0 if solr_rate is None else solr_rate * formula_soq
                                if market_sector_code == 'I':
                                    day_data['c10_rolling'] = solr_val_rolling
                                elif market_sector_code == 'D':
                                    day_data['c11_rolling'] = solr_val_rolling

                                day_data['c12_rolling'] = (c1_rolling * uni_gas_val) / 100
                                day_data['c13_rolling'] = ggl_fixed
                                day_data['c14_rolling'] = ggl_variable * rolling_alp_rate / 10
                                day_data['c15_rolling'] = xo_admin
                                day_data['c17_rolling'] = (timing_risk_val_list[i] if timing_risk_calc_flag else
                                                           timing_risk_val) * c1_rolling / 100
                                day_data['c18_rolling'] = c1_rolling * hedging_cost / 100
                                day_data['c19_rolling'] = c1_rolling * hedging_risk / 100
                                day_data['c20_rolling'] = c1_rolling * imb_cost / 100
                                day_data['c21_rolling'] = c1_rolling * imb_risk / 100
                                day_data['c22_rolling'] = rolling_alp_rate * risk_premium / 10
                                day_data['c23_rolling'] = rolling_alp_rate * energy_bal / 10
                                day_data['c24_rolling'] = rolling_alp_rate * gts_val / 10
                                day_data['c26_rolling'] = margin * rolling_alp_rate
                                day_data['c36_rolling'] = c1_rolling * timing_risk_monthly / 100

                                if calc_cts:
                                    day_data['c25_rolling'] = cts_unit_rolling * rolling_alp_rate
                                    day_data['c27_rolling'] = cts_standing_rolling / 365

                                day_data['c30_rolling'] = 0 if math.isnan(p_comm_unit_rate) else p_comm_unit_rate * \
                                    rolling_alp_rate
                                day_data['c31_rolling'] = 0 if math.isnan(p_comm_standing_charge) else (
                                        p_comm_standing_charge * 100 / 365)
                                day_data['c32_rolling'] = 0 if math.isnan(s_comm_unit_rate) else \
                                    s_comm_unit_rate * rolling_alp_rate
                                day_data['c33_rolling'] = 0 if math.isnan(s_comm_standing_charge) else \
                                    s_comm_standing_charge * 100 / 365
                                day_data['c34_rolling'] = 0 if math.isnan(t_comm_unit_rate) else \
                                    t_comm_unit_rate * rolling_alp_rate
                                day_data['c35_rolling'] = 0 if math.isnan(t_comm_standing_charge) else \
                                    t_comm_standing_charge * 100 / 365

                        if customer_alp_rate_cond:
                            # values dependent on customer aq
                            if len(fail_remarks) == 0:
                                c1_customer = customer_alp_rate * price_curve_value
                                day_data['c1_customer'] = c1_customer
                                day_data['c2_customer'] = customer_alp_rate * gnt_cost
                                day_data['c3_customer'] = ecn_cost * customer_soq

                                zco_rate = cost_calc_transport(transport_ranges["ZCO"], 'p/kWh',
                                                               formula_aq_conv,
                                                               customer_soq, zco_dict[i])
                                zca_cost = cost_calc_transport(transport_ranges["ZCA"],
                                                               'pence/peak day kWh/day',
                                                               formula_aq_conv, customer_soq, zca_dict[i]) * customer_soq
                                cca_cost = cost_calc_transport(transport_ranges["CCA"],
                                                               'pence/peak day kWh/day',
                                                               formula_aq_conv, customer_soq, cca_dict[i]) * customer_soq

                                # day_data['c4_customer'] = cost_calc_type1(zco_cost_1st_range, zco_cost_2nd_range,
                                #                                           zco_cost_3rd_range, formula_aq_conv,
                                #                                           customer_alp_rate, zco_exponent, zco_min,
                                #                                           customer_soq)
                                # day_data['c6_customer'] = \
                                #     cost_calc_type3(zca_cost_1st_range, zca_cost_2nd_range, zca_cost_3rd_range,
                                #                     formula_aq_conv, customer_alp_rate, zca_exponent, zca_min,
                                #                     customer_soq)
                                # day_data['c7_customer'] = \
                                #     cost_calc_type3(cca_cost_1st_range, cca_cost_2nd_range, cca_cost_3rd_range,
                                #                     formula_aq_conv, customer_alp_rate, cca_exponent, cca_min,
                                #                     customer_soq)

                                day_data['c4_customer'] = zco_rate * customer_alp_rate
                                day_data['c6_customer'] = zca_cost
                                day_data['c7_customer'] = cca_cost
                                day_data['c9_customer'] = cfi_cost

                                solr_val_cust = 0 if solr_rate is None else solr_rate * customer_soq
                                if market_sector_code == 'I':
                                    day_data['c10_customer'] = solr_val_cust
                                elif market_sector_code == 'D':
                                    day_data['c11_customer'] = solr_val_cust

                                day_data['c12_customer'] = (c1_customer * uni_gas_val) / 100

                                day_data['c13_customer'] = ggl_fixed
                                day_data['c14_customer'] = ggl_variable * customer_alp_rate / 10
                                day_data['c15_customer'] = xo_admin_dict[i]["Charge"]
                                day_data['c17_customer'] = (timing_risk_val_list[i] if timing_risk_calc_flag else
                                                            timing_risk_val) * c1_customer / 100
                                day_data['c18_customer'] = c1_customer * hedging_cost / 100
                                day_data['c19_customer'] = c1_customer * hedging_risk / 100
                                day_data['c20_customer'] = c1_customer * imb_cost / 100
                                day_data['c21_customer'] = c1_customer * imb_risk / 100
                                day_data['c22_customer'] = customer_alp_rate * risk_premium / 10
                                day_data['c23_customer'] = customer_alp_rate * energy_bal / 10
                                day_data['c24_customer'] = customer_alp_rate * gts_val / 10
                                day_data['c26_customer'] = margin * customer_alp_rate
                                day_data['c36_customer'] = c1_customer * timing_risk_monthly / 100

                                if calc_cts:
                                    day_data['c25_customer'] = cts_unit_cust * customer_alp_rate
                                    day_data['c27_customer'] = cts_standing_cust / 365

                                day_data['c30_customer'] = 0 if math.isnan(p_comm_unit_rate) else \
                                    p_comm_unit_rate * customer_alp_rate
                                day_data['c31_customer'] = 0 if math.isnan(p_comm_standing_charge) else \
                                    p_comm_standing_charge * 100 / 365
                                day_data['c32_customer'] = 0 if math.isnan(s_comm_unit_rate) else \
                                    s_comm_unit_rate * customer_alp_rate
                                day_data['c33_customer'] = 0 if math.isnan(s_comm_standing_charge) else \
                                    s_comm_standing_charge * 100 / 365
                                day_data['c34_customer'] = 0 if math.isnan(t_comm_unit_rate) else \
                                    t_comm_unit_rate * customer_alp_rate
                                day_data['c35_customer'] = 0 if math.isnan(t_comm_standing_charge) else \
                                    t_comm_standing_charge * 100 / 365

                    if len(fail_remarks) == 0:
                        pricing_dict[i] = day_data
                    else:
                        break

                if len(fail_remarks) == 0:
                    timing_risk_calc_flag = True
                    evaluation_data, daywise_data, error, status = \
                        create_price_evaluation(pricing_dict, is_hold, quote_period_days, holdRollingUnitRate,
                                                holdRollingStandingCharge, holdCustomerUnitRate,
                                                holdCustomerStandingCharge, cobd_val, copt_dict, payment_period,
                                                free_days, processing_days, processing_cost, day_wise_data_type)
                    if status:
                        pricing_status = "WARNING" if len(pricing_warnings) > 0 else "SUCCESS"
                        eval_to_save = PricingSegmentGroupEvaluationModelEO(
                            pricing_id=mprnData["PriceId"],
                            opportunity_master_id=mprnData["OpportunityMasterId"],
                            opp_segment_id=mprnData["SegmentId"],
                            group_id=mprnData["GroupId"],
                            mprn_id=mprnData["MprnId"],
                            pricing_status="WARNING" if len(pricing_warnings) > 0 else "SUCCESS",
                            pricing_remarks="&".join(pricing_warnings) if len(pricing_warnings) > 0 else "nan",
                            compressed_pricing_json=zlib.compress(daywise_data.encode('utf-8')),
                            is_compressed=True,
                            pricing_segment_group_evaluation_model_versions=[
                                PricingSegmentGroupEvaluationModelVersionEO(version=1,
                                                                            evaluation_json=
                                                                            json.dumps(evaluation_data,
                                                                                       cls=NpJsonEncoder),
                                                                            selected=True)
                            ],
                            selected=False
                        )
                        return eval_to_save, pricing_status

                        # return {
                        #     "pricingMasterId": mprnData["PriceId"],
                        #     "opportunityMasterId": mprnData["OpportunityMasterId"],
                        #     "pricingOpportunitySegmentMasterId": mprnData["SegmentId"],
                        #     "segmentId": mprnData["SegmentId"],
                        #     "groupId": mprnData["GroupId"],
                        #     "mprnId": mprnData["MprnId"],
                        #     "pricingStatus": "WARNING" if len(pricing_warnings) > 0 else "SUCCESS",
                        #     "pricingRemarks": "&".join(pricing_warnings) if len(pricing_warnings) > 0 else "nan",
                        #     "isHoldPrice": "Y" if is_hold else "N",
                        #     "pricingJson": daywise_data,
                        #     "pricingEvaluationModelVersions": [{
                        #             "evaluationJson": json.dumps(evaluation_data, cls=NpJsonEncoder),
                        #             "selected": 'true'
                        #         }]
                        # }
                    else:
                        fail_remarks += [error]
                        print(fail_remarks)
                        return save_remark_pricing_particular(mprnData, fail_remarks), "FAILED"
                else:
                    print(fail_remarks)
                    return save_remark_pricing_particular(mprnData, fail_remarks), "FAILED"
            else:
                print(fail_remarks)
                return save_remark_pricing_particular(mprnData, fail_remarks), "FAILED"
        else:
            print(input_remarks, failRemarks)
            return save_remark_pricing_particular(mprnData, input_remarks + failRemarks), "FAILED"
    except Exception as err:
        print(err)
        print(traceback.format_exc())
        return save_remark_pricing_particular(mprnData, [err]), "FAILED"


def save_pricing_status(pricingExecId, status, evalStatus, pricingCalcDuration,
                        pricingCompletedDuration, pricingCalcTime, isHold, userId):
    with app.app_context():
        pricing_status = PricingData.query.get(pricingExecId)
        pricing_status.pricingStatus = status
        pricing_status.pricingEndTime = datetime.datetime.now()
        pricing_status.pricingCalcTime = pricingCalcTime
        pricing_status.pricingCalcDuration = pricingCalcDuration
        pricing_status.pricingCompletedDuration = pricingCompletedDuration

        db.session.add(pricing_status)
        db.session.commit()

        pricingId = pricing_status.priceId

        update_eval_status_query = \
            text('UPDATE `pricing_mastereo` SET '
                 '`pricing_evaluation_status`=:evalStatus , `calculating`=:status, `last_priced_date_time`=:pricedTime '
                 'WHERE `pricing_id`=:pricingId')

        db.session.execute(update_eval_status_query, {'evalStatus': evalStatus,
                                                      'status': status,
                                                      'pricedTime': datetime.datetime.now(),
                                                      'pricingId': pricingId
                                                      })
        db.session.commit()
        try:
            ra_engine_log_update_query = \
                text('UPDATE `raengine_logeo` SET `execution_status`=:priceStatus, '
                     '`process_end_time`=:currTime WHERE `iras_execution_id`=:execId')
            db.session.execute(ra_engine_log_update_query,
                               {'priceStatus': status,
                                'currTime': datetime.datetime.now(),
                                'execId': pricingExecId})
            db.session.commit()
        except Exception as e:
            pass

        send_price_status(
            pricingId, "PRICED" if (evalStatus == "SUCCESS" or evalStatus == "WARNING") else "PRICING_FAILED", userId
        )

        if isHold:
            send_for_hold_price_evaluation(pricingId, userId)


def get_forecast_data(executionId, mprnId):
    with app.app_context():
        return pd.read_feather(os.path.join(app.config['FORECAST_OUT'], executionId, mprnId + '.feather'))


@ray.remote
def pre_pricing(mprn_data, unique_mprn_count, price_curve_data_dict, ecn_data, gnt_data, zco_data, zca_data,
                cca_data, cfi_data, solr_data, uni_gas_data, transport_ranges, uni_gas_ranges, ggl_fixed_data,
                ggl_variable_data, xo_admin_data, timing_risk_data, hedging_cost_data, timing_risk_monthly_data,
                hedging_risk_data, imb_cost_data,
                imb_risk_data, risk_premium_data, energy_balance_data, gas_trading_data, cts_dict, cts_aq_ranges,
                cts_mprn_ranges, cobd_cra_ranges, cobd_pb_ranges, cobd_dict, copt_data, primary_tpi_dict,
                secondary_tpi_dict, tertiary_tpi_dict, startDate, endDate, executionId, pricingExecId, isHold, userId):
    t = Timer()
    t.start()

    try:
        eval_status = set()
        pricingId = mprn_data[0]['PriceId']
        quote_period_days = cal_days_diff(endDate, startDate)
        gnt_dict = truncate_df_bet_dates_to_dict(gnt_data, startDate, endDate)
        ggl_fixed_dict = truncate_df_bet_dates_to_dict(ggl_fixed_data, startDate, endDate)
        ggl_variable_dict = truncate_df_bet_dates_to_dict(ggl_variable_data, startDate, endDate)
        xo_admin_dict = truncate_df_bet_dates_to_dict(xo_admin_data, startDate, endDate)

        hedging_cost_dict = truncate_df_bet_dates_to_dict(hedging_cost_data, startDate, endDate)
        timing_risk_monthly_dict = truncate_df_bet_dates_to_dict(timing_risk_monthly_data, startDate, endDate)
        hedging_risk_dict = truncate_df_bet_dates_to_dict(hedging_risk_data, startDate, endDate)

        imb_cost_dict = truncate_df_bet_dates_to_dict(imb_cost_data, startDate, endDate)
        imb_risk_dict = truncate_df_bet_dates_to_dict(imb_risk_data, startDate, endDate)

        risk_premium_dict = truncate_df_bet_dates_to_dict(risk_premium_data, startDate, endDate)
        energy_balance_dict = truncate_df_bet_dates_to_dict(energy_balance_data, startDate, endDate)
        gas_trading_dict = truncate_df_bet_dates_to_dict(gas_trading_data, startDate, endDate)

        copt_dict = truncate_df_bet_dates_to_dict_with_key(copt_data, 'Effective_Month', startDate, endDate)

        @ray.remote
        def price_chunks(chunkData):
            pricingData = []
            for j in chunkData:
                fail_remarks = set()
                mprn_ = j

                ldz_id = mprn_['LDZ']
                exit_zone = mprn_['ExitZone']

                # forecast_df, rolling_aq_sum, customer_aq_sum = get_forecast_mprn(mprn_["MprnId"],
                #                                                                  mprn_["OpportunityMasterId"],
                #                                                                  executionId)

                try:
                    forecast_df = get_forecast_data(str(executionId), mprn_["MprnId"])
                except FileNotFoundError:
                    fail_remarks.add("E_FCAST_D")

                if len(fail_remarks) == 0:
                    pricing_df = forecast_df.drop(['Alp Rates'], axis=1)
                    pricing_df['Date'] = pd.to_datetime(pricing_df['Date'], format="%Y-%m-%d")
                    pricing_df.set_index('Date', inplace=True)
                    pricing_df = pricing_df.loc[startDate:endDate].reset_index()

                    rolling_aq_sum = pricing_df['Rolling AQ'].sum()
                    customer_aq_sum = pricing_df['Customer AQ'].sum()

                    pricing_dict = pricing_df.to_dict('records')

                    ecn_dict = truncate_df_bet_dates_to_dict(
                        ecn_data[(ecn_data['LDZ'] == ldz_id) & (ecn_data['Exit Zone'] == exit_zone)].reset_index(
                            drop=True).drop(['index'], axis=1), startDate, endDate)
                    if len(ecn_dict) == 0:
                        fail_remarks.add('E_LdzExz_D')
                    cfi_dict = truncate_df_bet_dates_to_dict(cfi_data[cfi_data['LDZ'] == ldz_id]
                                                             .reset_index(drop=True), startDate, endDate)
                    if len(cfi_dict) == 0:
                        fail_remarks.add('E_LDZ_CFI_D')
                    solr_dict = truncate_df_bet_dates_to_dict(solr_data[solr_data['LDZ'] == ldz_id]
                                                              .reset_index(drop=True), startDate, endDate)
                    if len(solr_dict) == 0:
                        fail_remarks.add('E_LDZ_SOLR_D')
                    zco_dict = truncate_df_bet_dates_to_dict(zco_data[zco_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(zco_dict) == 0:
                        fail_remarks.add('E_LDZ_ZCO_D')
                    zca_dict = truncate_df_bet_dates_to_dict(zca_data[zca_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(zca_dict) == 0:
                        fail_remarks.add('E_LDZ_ZCA_D')
                    cca_dict = truncate_df_bet_dates_to_dict(cca_data[cca_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(cca_dict) == 0:
                        fail_remarks.add('E_LDZ_CCA_D')
                    uni_gas_dict = truncate_df_bet_dates_to_dict(uni_gas_data[uni_gas_data['LDZ'] == ldz_id],
                                                                 startDate, endDate)
                    if len(uni_gas_dict) == 0:
                        fail_remarks.add('E_LDZ_UNI_D')

                    price_to_save, pricing_status = calculate_pricing(mprn_, unique_mprn_count, quote_period_days,
                                                                      rolling_aq_sum, customer_aq_sum, pricing_dict,
                                                                      price_curve_data_dict, gnt_dict, ecn_dict,
                                                                      zco_dict, zca_dict, cca_dict, cfi_dict, solr_dict,
                                                                      uni_gas_dict, transport_ranges, uni_gas_ranges,
                                                                      ggl_fixed_dict, ggl_variable_dict, xo_admin_dict,
                                                                      timing_risk_data, hedging_cost_dict,
                                                                      timing_risk_monthly_dict,
                                                                      hedging_risk_dict, imb_cost_dict,
                                                                      imb_risk_dict, risk_premium_dict,
                                                                      energy_balance_dict, gas_trading_dict, cts_dict,
                                                                      cts_aq_ranges, cts_mprn_ranges, cobd_cra_ranges,
                                                                      cobd_pb_ranges, cobd_dict, copt_dict,
                                                                      primary_tpi_dict, secondary_tpi_dict,
                                                                      tertiary_tpi_dict, list(fail_remarks))
                else:
                    price_to_save, pricing_status = save_remark_pricing_particular(mprn_, fail_remarks), "FAILED"

                pricingData += [price_to_save]
                eval_status.add(pricing_status)

            return pricingData

        total_mprns = len(mprn_data)

        if total_mprns > 350:
            t2 = Timer()
            t2.start()
            exec_pricing = [price_chunks.remote(mprn_data[i:i+350]) for i in range(0, total_mprns, 350)]
            pricingData = [item for sublist in ray.get(exec_pricing) for item in sublist]
            t2.stop()
            pricing_calc_time = datetime.datetime.now()
            print(pricingExecId, ":: Calculation Completed in", t2.elapsed(), "seconds")
            if sendPricingData(pricingExecId, pricingData, pricingId):
                status = "SUCCESS"
            else:
                status = "FAILED"
            t.stop()
            print(pricingExecId, ":: Sent to CPQ", t.elapsed())
            save_pricing_status(pricingExecId, status,  get_eval_status(eval_status),
                                t2.elapsed(), t.elapsed(), pricing_calc_time, isHold, userId)
        else:
            t1 = Timer()
            t1.start()
            pricingData = []
            for j in mprn_data:
                fail_remarks = set()
                mprn_ = j
                ldz_id = mprn_['LDZ']
                exit_zone = mprn_['ExitZone']
                # forecast_df, rolling_aq_sum, customer_aq_sum = get_forecast_mprn(mprn_["MprnId"],
                #                                                                  mprn_["OpportunityMasterId"],
                #                                                                  executionId)

                try:
                    forecast_df = get_forecast_data(str(executionId), mprn_["MprnId"])
                except FileNotFoundError:
                    fail_remarks.add("E_FCAST_D")

                if len(fail_remarks) == 0:
                    pricing_df = forecast_df.drop(['Alp Rates'], axis=1)
                    pricing_df['Date'] = pd.to_datetime(pricing_df['Date'], format="%Y-%m-%d")
                    pricing_df.set_index('Date', inplace=True)
                    pricing_df = pricing_df.loc[startDate:endDate].reset_index()

                    rolling_aq_sum = pricing_df['Rolling AQ'].sum()
                    customer_aq_sum = pricing_df['Customer AQ'].sum()

                    pricing_dict = pricing_df.to_dict('records')

                    ecn_dict = truncate_df_bet_dates_to_dict(
                        ecn_data[(ecn_data['LDZ'] == ldz_id) & (ecn_data['Exit Zone'] == exit_zone)].reset_index(
                            drop=True).drop(['index'], axis=1), startDate, endDate)
                    if len(ecn_dict) == 0:
                        fail_remarks.add('E_LdzExz_D')
                    cfi_dict = truncate_df_bet_dates_to_dict(cfi_data[cfi_data['LDZ'] == ldz_id]
                                                             .reset_index(drop=True), startDate, endDate)
                    if len(cfi_dict) == 0:
                        fail_remarks.add('E_LDZ_CFI_D')
                    solr_dict = truncate_df_bet_dates_to_dict(solr_data[solr_data['LDZ'] == ldz_id]
                                                              .reset_index(drop=True), startDate, endDate)
                    if len(solr_dict) == 0:
                        fail_remarks.add('E_LDZ_SOLR_D')
                    zco_dict = truncate_df_bet_dates_to_dict(zco_data[zco_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(zco_dict) == 0:
                        fail_remarks.add('E_LDZ_ZCO_D')
                    zca_dict = truncate_df_bet_dates_to_dict(zca_data[zca_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(zca_dict) == 0:
                        fail_remarks.add('E_LDZ_ZCA_D')
                    cca_dict = truncate_df_bet_dates_to_dict(cca_data[cca_data['LDZ'] == ldz_id], startDate, endDate)
                    if len(cca_dict) == 0:
                        fail_remarks.add('E_LDZ_CCA_D')
                    uni_gas_dict = truncate_df_bet_dates_to_dict(uni_gas_data[uni_gas_data['LDZ'] == ldz_id],
                                                                 startDate, endDate)
                    if len(uni_gas_dict) == 0:
                        fail_remarks.add('E_LDZ_UNI_D')

                    price_to_save, pricing_status = calculate_pricing(mprn_, unique_mprn_count, quote_period_days,
                                                                      rolling_aq_sum, customer_aq_sum, pricing_dict,
                                                                      price_curve_data_dict, gnt_dict, ecn_dict,
                                                                      zco_dict, zca_dict, cca_dict, cfi_dict, solr_dict,
                                                                      uni_gas_dict, transport_ranges, uni_gas_ranges,
                                                                      ggl_fixed_dict, ggl_variable_dict, xo_admin_dict,
                                                                      timing_risk_data, hedging_cost_dict,
                                                                      timing_risk_monthly_dict,
                                                                      hedging_risk_dict, imb_cost_dict,
                                                                      imb_risk_dict, risk_premium_dict,
                                                                      energy_balance_dict, gas_trading_dict, cts_dict,
                                                                      cts_aq_ranges, cts_mprn_ranges, cobd_cra_ranges,
                                                                      cobd_pb_ranges, cobd_dict, copt_dict,
                                                                      primary_tpi_dict, secondary_tpi_dict,
                                                                      tertiary_tpi_dict, list(fail_remarks))

                else:
                    price_to_save, pricing_status = save_remark_pricing_particular(mprn_, fail_remarks), "FAILED"

                pricingData += [price_to_save]
                eval_status.add(pricing_status)
            t1.stop()
            pricing_calc_time = datetime.datetime.now()
            print(pricingExecId, ":: Calculation Completed in", t1.elapsed(), "seconds")
            if sendPricingData(pricingExecId, pricingData, pricingId):
                status = "SUCCESS"
            else:
                status = "FAILED"
            t.stop()
            print(pricingExecId, ":: Sent to CPQ", t.elapsed())
            save_pricing_status(pricingExecId, status, get_eval_status(eval_status),
                                t1.elapsed(), t.elapsed(), pricing_calc_time, isHold, userId)

    except Exception as e:
        print(pricingExecId, ":: Pricing Failed", e)
        t.stop()
        print(pricingExecId, ":: Sent to CPQ", t.elapsed())
        save_pricing_status(pricingExecId, "FAILED", None, t.elapsed(), t.elapsed(),
                            datetime.datetime.now(), isHold, userId)


def truncate_df_bet_dates_to_dict(df, startDate, endDate):
    return df[df['Date'].between(startDate, endDate)].reset_index(drop=True).to_dict('records')


def truncate_df_bet_dates(df, startDate, endDate):
    return df[df['Date'].between(startDate, endDate)].reset_index(drop=True)


def truncate_df_bet_dates_to_dict_with_key(df, key, startDate, endDate):
    startDate = startDate.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    endDate = endDate.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return df[df[key].between(startDate, endDate)].reset_index(drop=True).to_dict('records')


def get_forecast_mprn(mprnId, oppId, executionId):
    with app.app_context():
        mprn_forecast = get_mprn_forecast(mprnId, oppId, executionId)
        return pd.json_normalize(json.loads(mprn_forecast.forecastJson)).reset_index(
            drop=True), mprn_forecast.totalRollingAq,  mprn_forecast.totalCustomerAq


def save_remark_pricing_particular(mprn_, pricing_fail_remarks):
    remark = "&".join(pricing_fail_remarks)

    return PricingSegmentGroupEvaluationModelEO(
        pricing_id=mprn_["PriceId"],
        opportunity_master_id=mprn_["OpportunityMasterId"],
        opp_segment_id=mprn_["SegmentId"],
        group_id=mprn_["GroupId"],
        mprn_id=mprn_["MprnId"],
        pricing_status="FAILED",
        pricing_remarks=remark,
        compressed_pricing_json=zlib.compress(json.dumps([]).encode('utf-8')),
        is_compressed=True,
        pricing_segment_group_evaluation_model_versions=[
            PricingSegmentGroupEvaluationModelVersionEO(version=1,
                                                        evaluation_json=json.dumps([]),
                                                        selected=True)
        ],
        selected=False
    )
    # return {
    #     "pricingMasterId": mprn_["PriceId"],
    #     "opportunityMasterId": mprn_["OpportunityMasterId"],
    #     "pricingOpportunitySegmentMasterId": mprn_["SegmentId"],
    #     "segmentId": mprn_["SegmentId"],
    #     "groupId": mprn_["GroupId"],
    #     "mprnId": mprn_["MprnId"],
    #     "pricingStatus": "FAILED",
    #     "pricingRemarks": remark,
    #     "isHoldPrice": "N",
    #     "pricingJson": json.dumps([]),
    #     "pricingEvaluationModelVersions": [{"evaluationJson": json.dumps([]), "selected": 'true'}]
    # }


@ray.remote
def save_remark_pricing(pricingExecId, mprn_data, pricing_fail_remarks, isHold, userId):
    t = Timer()
    t.start()
    pricingData = []
    remark = "&".join(pricing_fail_remarks)
    pricingId = mprn_data[0]["PriceId"]
    for i in mprn_data:
        pricingData += [
            PricingSegmentGroupEvaluationModelEO(
                pricing_id=i["PriceId"],
                opportunity_master_id=i["OpportunityMasterId"],
                opp_segment_id=i["SegmentId"],
                group_id=i["GroupId"],
                mprn_id=i["MprnId"],
                pricing_status="FAILED",
                pricing_remarks=remark,
                compressed_pricing_json=zlib.compress(json.dumps([]).encode('utf-8')),
                is_compressed=True,
                pricing_segment_group_evaluation_model_versions=[
                    PricingSegmentGroupEvaluationModelVersionEO(version=1,
                                                                evaluation_json=json.dumps([]),
                                                                selected=True)
                ],
                selected=False
            )]
        # pricingData += [{
        #     "pricingMasterId": i["PriceId"],
        #     "opportunityMasterId": i["OpportunityMasterId"],
        #     "pricingOpportunitySegmentMasterId": i["SegmentId"],
        #     "segmentId": i["SegmentId"],
        #     "groupId": i["GroupId"],
        #     "mprnId": i["MprnId"],
        #     "pricingStatus": "FAILED",
        #     "pricingRemarks": remark,
        #     "isHoldPrice": "N",
        #     "pricingJson": json.dumps([]),
        #     "pricingEvaluationModelVersions": [{"evaluationJson": json.dumps([]), "selected": 'true'}]
        # }]
    if sendPricingData(pricingExecId, pricingData, pricingId):
        status = "SUCCESS"
    else:
        status = "FAILED"
    t.stop()
    print(pricingExecId, ":: Sent to CPQ", t.elapsed())
    save_pricing_status(pricingExecId, status, "FAILED", t.elapsed(), t.elapsed(),
                        datetime.datetime.now(), isHold, userId)

