import datetime
import json
import math
import os

import pandas as pd
import ray
from sqlalchemy import text

from configure.config import app
from models.models import db, ForecastData, ForecastStatus
from services.common.timer import Timer
from services.services import send_customer_soq_calc


def cal_days_diff(a, b):
    a_ = a.replace(hour=0, minute=0, second=0, microsecond=0)
    b_ = b.replace(hour=0, minute=0, second=0, microsecond=0)
    return (a_ - b_).days + 1


def save_forecast(forecast_to_save, executionId, status, total_rolling_aq, total_customer_aq):
    with app.app_context():
        db.session.bulk_save_objects(forecast_to_save)
        db.session.commit()
        forecast_status = ForecastStatus.query.get(int(executionId))
        forecast_status.forecastStatus = status
        forecast_status.totalRollingAq = total_rolling_aq
        forecast_status.totalCustomerAq = total_customer_aq
        forecast_status.forecastedAt = datetime.datetime.now()
        forecast_status.forecastCalcEndTime = datetime.datetime.now()
        db.session.add(forecast_status)
        db.session.commit()
        update_opportunity_status = \
            text("UPDATE `opportunity_mastereo` SET `forecast_status`=:status WHERE id=:opportunityMasterId")
        db.session.execute(update_opportunity_status, {'status': status,
                                                       'opportunityMasterId': forecast_status.opportunityMasterId})
        db.session.commit()
        update_raengine_log = \
            text("UPDATE `raengine_logeo` SET `execution_status`=:status WHERE `iras_execution_id`=:executionId")
        db.session.execute(update_raengine_log, {'status': status,
                                                 'executionId': str(executionId)})
        db.session.commit()
        if to_calc_cust_aq():
            send_customer_soq_calc(forecast_status.opportunityMasterId)


def calculate_forecast(forecast_alp_dict, euc, rollingAQ, customerAQ, is_rolling_aq_valid, is_customer_aq_valid):
    forecast_data = []
    fail_remarks = []
    total_rolling_aq = 0
    total_customer_aq = 0

    for i in forecast_alp_dict:
        day_forecast = {}
        day_alp = i
        try:
            alp_rate = float(day_alp[euc])
        except KeyError:
            fail_remarks += ['Could not find ALP data for EUC Code ' + euc]

        if len(fail_remarks) > 0:
            break
        else:
            day_rolling_aq = float(rollingAQ) * alp_rate / 365 if is_rolling_aq_valid else ''
            day_customer_aq = float(customerAQ) * alp_rate / 365 if is_customer_aq_valid else ''
            total_rolling_aq += day_rolling_aq if day_rolling_aq != '' else 0
            total_customer_aq += day_customer_aq if day_customer_aq != '' else 0

            day_forecast["Date"] = day_alp['DATE'].strftime('%Y-%m-%d')
            day_forecast['Alp Rates'] = alp_rate
            day_forecast['Rolling AQ'] = day_rolling_aq if day_rolling_aq != '' else ''
            day_forecast['Customer AQ'] = day_customer_aq if day_customer_aq != '' else ''

            forecast_data += [day_forecast]

    return forecast_data, total_rolling_aq, total_customer_aq, fail_remarks


def logMessage(executionId, timetaken):
    print("Forecast:: Execution Id:", executionId, "execution in", timetaken, "seconds")


def save_forecast_to_file(data, executionId, mprnId):
    with app.app_context():
        pd.DataFrame(data).reset_index().to_feather(os.path.join(app.config['FORECAST_OUT'], executionId,
                                                                 mprnId + '.feather'))


# def calculate_customer_soq(mprnId, customer_aq, load_factor_mprn):
#     return {
#         "mprnId": mprnId,
#         "lovGroupName": "PROFILE_ATTRIBUTE",
#         "mprnLovName": "Customer SOQ",
#         "mprnLovValue": str((customer_aq / 365) * load_factor_mprn)
#     }


def to_calc_cust_aq():
    return app.config.get("CUST_SOQ_CAL")


@ray.remote
def pre_forecast(mprnData, processed_alp, startDate, endDate, executionId, alpDataUsed, forcast_band_ranges):
    t = Timer()
    t.start()
    # calc_cust_aq = to_calc_cust_aq()
    try:
        forecast_alp_dict = processed_alp[processed_alp['DATE'].between(startDate, endDate)]\
            .reset_index(drop=True).to_dict('records')

        alp_start_date = forecast_alp_dict[0]['DATE']
        alp_end_date = forecast_alp_dict[-1]['DATE']

        delta = cal_days_diff(endDate, startDate)
        status_ = set()
        dateRemark = []
        if not (alp_start_date == startDate and alp_end_date == endDate):
            dateRemark += ['Quote Period exceed to ALP dates']
        if len(forecast_alp_dict) != delta:
            dateRemark += ['ALP Data Missing for few days']

        forecast_to_save = []
        for i in mprnData:
            mprn_ = i
            remarks = []
            calc_remarks = []
            status = 'SUCCESS'
            remark_all = []
            mprnId = mprn_['Mprn Id']
            forecast_data, total_rolling_aq, total_customer_aq = None, None, None
            if len(dateRemark) == 0:
                euc = mprn_['End Usr Cat Code']
                rolling_aq = mprn_['Annual Quantity']
                customer_aq = mprn_['Customer AQ']
                threshold_forecast = mprn_['Threshold Forecast']
                euc_band = None
                isEUCValidated = True

                if euc is None or euc == '':
                    isEUCValidated = False
                    remarks += ['EUC Code missing for MPRN']
                else:
                    try:
                        euc_band = int(euc[7])
                    except IndexError:
                        pass

                is_rolling_aq_valid = is_float(rolling_aq)
                is_customer_aq_valid = is_float(customer_aq)

                if (rolling_aq is None or not is_rolling_aq_valid) and \
                        (customer_aq is None or not is_customer_aq_valid):
                    remarks += ['Both Rolling AQ and Customer AQ are not available or not valid']
                if mprn_['isDuplicate']:
                    remarks += ['Duplicate MPRN found. Please remove duplicate MPRN and recalculate forecast']

                if rolling_aq == '':
                    calc_remarks += ["Rolling AQ is not available"]
                elif not is_rolling_aq_valid:
                    calc_remarks += ["Rolling AQ is not valid"]
                elif float(rolling_aq) == 0:
                    calc_remarks += ["Rolling AQ is 0"]
                elif float(rolling_aq) == 1:
                    calc_remarks += ["Rolling AQ is 1"]
                else:
                    pass

                if customer_aq == '':
                    pass
                elif not is_customer_aq_valid:
                    calc_remarks += ["Customer AQ is not valid"]
                elif float(customer_aq) == 0:
                    calc_remarks += ["Customer AQ is 0"]
                elif float(customer_aq) == 1:
                    calc_remarks += ["Customer AQ is 1"]
                else:
                    pass

                if euc_band is not None:
                    for j in forcast_band_ranges:
                        if j['EUC_BAND'] == euc_band:
                            cond1 = (not j['AQBAND_START_VALUE'] <= float(rolling_aq) <=
                                    j['AQBAND_END_VALUE']) if is_rolling_aq_valid else False
                            cond2 = (not j['AQBAND_START_VALUE'] <= float(customer_aq) <=
                                         j['AQBAND_END_VALUE']) if is_customer_aq_valid else False
                            if cond1 and cond2:
                                calc_remarks += ["Rolling AQ and Customer AQ value is Outside Band Range"]
                            elif cond1:
                                calc_remarks += ["Rolling AQ value is Outside Band Range"]
                            elif cond2:
                                calc_remarks += ["Customer AQ value is Outside Band Range"]
                            break

                if is_rolling_aq_valid and float(rolling_aq) > float(threshold_forecast):
                    calc_remarks += ["Rolling AQ is > Threshold value (40.0Gwh)"]
                if is_customer_aq_valid and float(customer_aq) > float(threshold_forecast):
                    calc_remarks += ["Customer AQ is > Threshold value (40.0Gwh)"]

                if isEUCValidated:
                    forecast_data, total_rolling_aq, total_customer_aq, fail_remarks = \
                        calculate_forecast(forecast_alp_dict, euc, rolling_aq, customer_aq,
                                           is_rolling_aq_valid, is_customer_aq_valid)

                    save_forecast_to_file(forecast_data, str(executionId), mprn_['Mprn Id'])
                    remarks += fail_remarks

                if len(remarks) == 0:
                    remark_all += ['Warning: ' + ' ; '.join(calc_remarks)] if len(calc_remarks) > 0 else []
                else:
                    remark_all += ['Failed: ' + ' ; '.join(remarks)]
                    status = 'FAILED'
            else:
                remark_all = ['Failed: ' + ' ; '.join(dateRemark)]
                status = 'FAILED'

            forecast_to_save += [ForecastData(
                opportunityMasterId=mprn_['Opportunity Master Id'],
                opportunityId=mprn_['Opportunity Id'],
                mprnId=mprnId,
                forecastJson=json.dumps(forecast_data),
                forecastStartDate=startDate.date(),
                forecastEndDate=endDate.date(),
                forecastDays=delta,
                forecastStatus=status,
                forecastRemark='&'.join(remark_all),
                totalRollingAq=total_rolling_aq,
                totalCustomerAq=total_customer_aq,
                alpDataUsed=alpDataUsed,
                forecastedAt=datetime.datetime.now(),
                executionId=executionId
            )]

            status_.add(status)

        save_forecast(forecast_to_save, executionId,
                      "SUCCESS" if "FAILED" not in status_ else "FAILED",
                      total_rolling_aq, total_customer_aq)
        t.stop()
        logMessage(executionId, t.elapsed())
    except Exception as e:
        t.stop()
        print(executionId, ":: Forecast Failed", e)
        save_forecast([], executionId, "FAILED", None, None)


@ray.remote
def save_duplicate_mprn_fail_forecast(mprnData, startDate, endDate, executionId):
    t = Timer()
    t.start()
    forecast_to_save = []
    delta = cal_days_diff(endDate, startDate)
    for i in mprnData:
        mprn_ = i
        remarks = []
        forecast_data, total_rolling_aq, total_customer_aq, calc_remarks = None, None, None, None
        if i['isDuplicate']:
            remarks += ['Failed: Duplicate MPRN found. Please remove duplicate MPRN and recalculate forecast']
        forecast_to_save += [ForecastData(
            opportunityMasterId=mprn_['Opportunity Master Id'],
            opportunityId=mprn_['Opportunity Id'],
            mprnId=mprn_['Mprn Id'],
            forecastJson=forecast_data,
            forecastStartDate=startDate.date(),
            forecastEndDate=endDate.date(),
            forecastDays=delta,
            forecastStatus="Failed",
            forecastRemark='&'.join(remarks),
            totalRollingAq=total_rolling_aq,
            totalCustomerAq=total_customer_aq,
            alpDataUsed=None,
            forecastedAt=datetime.datetime.now(),
            executionId=executionId
        )]
    save_forecast(forecast_to_save, executionId, "FAILED", total_rolling_aq, total_customer_aq)
    t.stop()
    logMessage(executionId, t.elapsed())


def is_float(text):
    # check for nan/infinity etc.
    if text.isalpha():
        return False
    try:
        float(text) and not math.isnan(float(text))
        return True
    except ValueError:
        return False
