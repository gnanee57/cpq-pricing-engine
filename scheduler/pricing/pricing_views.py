import json
import math
import os
import pickle
from datetime import datetime
from io import StringIO

import pandas as pd
from flask import request, jsonify

from configure.config import app
from forecast.forecast_views import get_latest_forecast_status
from models.models import PricingData, db, PricingSegmentGroupEvaluationModelVersionEO, declarative_base
from preprocess.preprocess import get_preprocessed_file
from pricing.pricing import pre_pricing, save_remark_pricing
from services.common.encoder import NpJsonEncoder
from services.services import send_price_status


def pricing():
    if request.method == "POST":
        payload = json.loads(request.data.decode("utf-8"))
        mprn_csv = payload["reconName"]
        mprn_csv_io = StringIO(mprn_csv)
        opportunityId = payload["opportunityId"]
        priceId = payload["priceId"]
        start_date = datetime.strptime(payload["startDate"], '%Y-%m-%d')
        end_date = datetime.strptime(payload["endDate"], '%Y-%m-%d')
        start_date_ = start_date.date()
        end_date_ = end_date.date()
        price_curve_file_name = payload["priceCurveFilename"]
        alp_file_name = payload["alpFilename"]
        primaryTpiId = payload["Primary_TPI_Id"]
        secondaryTpiId = payload["Secondary_TPI_Id"]
        tertiaryTpiId = payload["Tertiary_TPI_Id"]
        isTpiPresent = payload["ThirdPartyIndicator"]
        isHold = True if payload["isHold"] == "Y" else False
        userId = payload["calculationTriggeredByUserId"]

        mprn_df = pd.read_csv(mprn_csv_io, sep='|', lineterminator='~', index_col=False,
                              converters={'MprnId': str})
        unique_mprn_count = len(pd.unique(mprn_df['MprnId']))

        mprn_data = mprn_df.to_dict('records')

        pricing_data = PricingData(
            opportunityId=opportunityId,
            priceId=priceId,
            pricingStartDate=start_date.date(),
            pricingEndDate=end_date.date(),
            totalMPRNS=len(mprn_data),
            uniqueMPRNS=unique_mprn_count,
            pricingStartTime=datetime.now(),
            pricingStatus="SUBMITTED"
        )

        db.session.add(pricing_data)
        db.session.commit()
        exec_id = pricing_data.id

        with open(os.path.join(app.config['PRICING_INPUT'],
                               str(exec_id) + '_' + str(opportunityId) + '_' + str(priceId) + '.csv'),
                  'w') as out:
            out.write(mprn_csv.replace("~", "\n"))

        forecast_status = get_latest_forecast_status(mprn_data[0]["OpportunityMasterId"])
        if forecast_status is None:
            save_remark_pricing.remote(exec_id, mprn_data, ["E_FCAST_D"], isHold)
        else:
            forecast_start_date = forecast_status.forecastStartDate
            forecast_end_date = forecast_status.forecastEndDate

            file_path = os.path.join(app.config['PREPROCESS_DIR'], "PRICECURVE", price_curve_file_name)
            price_curve_data = pd.read_csv(file_path, sep='|')
            price_curve_data['Date_RANGE'] = pd.to_datetime(price_curve_data['Date_RANGE'], format="%Y-%m-%d")

            price_curve_data_dict = price_curve_data[price_curve_data['Date_RANGE'].between(start_date, end_date)] \
                .to_dict('records')
            price_curve_start_date = price_curve_data_dict[0]['Date_RANGE']
            price_curve_end_date = price_curve_data_dict[-1]['Date_RANGE']
            print(price_curve_file_name, price_curve_start_date, price_curve_end_date)

            ecn = get_preprocessed_file("ECN")
            gnt = get_preprocessed_file("GNT")
            zco = get_preprocessed_file("ZCO")
            zca = get_preprocessed_file("ZCA")
            cca = get_preprocessed_file("CCA")
            cfi = get_preprocessed_file("CFI")
            solr = get_preprocessed_file("SoLR")
            uni_gas = get_preprocessed_file("UNI_GAS")
            ggl_fixed = get_preprocessed_file("GGL_Fixed")
            ggl_variable = get_preprocessed_file("GGL_Variable")
            xo = get_preprocessed_file("XO_ADMIN")
            hedging_cost = get_preprocessed_file("Hedging_Cost")
            hedging_risk = get_preprocessed_file("Hedging_Risk")
            imb_cost = get_preprocessed_file("IMB_Cost")
            imb_risk = get_preprocessed_file("IMB_Risk")
            risk = get_preprocessed_file("RiskPremium")
            energy_bal = get_preprocessed_file("EnergyBalance")
            gas_trade = get_preprocessed_file("Gas_Trade")
            copt = get_preprocessed_file("COPT")
            tpi_limits_data = pickle.loads(get_preprocessed_file("TPI_Limits").dataFrame)

            pricing_fail_remarks = []

            validate_input = {
                'E_PRICE': (price_curve_start_date.date(), price_curve_end_date.date()),
                'E_FORECAST': (forecast_start_date, forecast_end_date),
                "E_UNIGAS": (uni_gas.dataFrameStartDate, uni_gas.dataFrameEndDate),
                "E_TRANSPORT": (ecn.dataFrameStartDate, ecn.dataFrameEndDate),
                "E_RISK": (risk.dataFrameStartDate, risk.dataFrameEndDate),
                "E_GFIXED": (ggl_fixed.dataFrameStartDate, ggl_fixed.dataFrameEndDate),
                "E_GTRADE": (gas_trade.dataFrameStartDate, gas_trade.dataFrameEndDate),
                "E_XO": (xo.dataFrameStartDate, xo.dataFrameEndDate),
                "E_HEDGEC": (hedging_cost.dataFrameStartDate, hedging_cost.dataFrameEndDate),
                "E_HEDGER": (hedging_risk.dataFrameStartDate, hedging_risk.dataFrameEndDate),
                "E_GVAR": (ggl_variable.dataFrameStartDate, ggl_variable.dataFrameEndDate),
                "E_BAL": (energy_bal.dataFrameStartDate, energy_bal.dataFrameEndDate),
                "E_IMBR": (imb_risk.dataFrameStartDate, imb_risk.dataFrameEndDate),
                "E_IMBC": (imb_cost.dataFrameStartDate, imb_cost.dataFrameEndDate),
                "E_COPT": (copt.dataFrameStartDate, copt.dataFrameEndDate)
            }

            for i in validate_input.keys():
                df_start_date, df_end_date = validate_input[i][0], validate_input[i][1]
                if not (df_start_date <= start_date_ and df_end_date >= end_date_):
                    pricing_fail_remarks += [i]

            if isTpiPresent == 'Y':
                if primaryTpiId is not None and primaryTpiId != "":
                    primary_tpi_dict = tpi_limits_data[tpi_limits_data['TPI_ID'] == int(primaryTpiId)].to_dict(
                        'records')
                    if len(primary_tpi_dict) == 0:
                        pricing_fail_remarks += ['E_PTPI']
                else:
                    primary_tpi_dict = []
                if secondaryTpiId is not None and secondaryTpiId != "":
                    secondary_tpi_dict = tpi_limits_data[tpi_limits_data['TPI_ID'] == int(secondaryTpiId)].to_dict(
                        'records')
                    if len(secondary_tpi_dict) == 0:
                        pricing_fail_remarks += ['E_STPI']
                else:
                    secondary_tpi_dict = []
                if tertiaryTpiId is not None and tertiaryTpiId != "":
                    tertiary_tpi_dict = tpi_limits_data[tpi_limits_data['TPI_ID'] == int(tertiaryTpiId)].to_dict(
                        'records')
                    if len(tertiary_tpi_dict) == 0:
                        pricing_fail_remarks += ['E_TTPI']
                else:
                    tertiary_tpi_dict = []
            else:
                primary_tpi_dict = []
                secondary_tpi_dict = []
                tertiary_tpi_dict = []

            if len(pricing_fail_remarks) > 0:
                save_remark_pricing.remote(exec_id, mprn_data, pricing_fail_remarks, isHold, userId)
                status = "SUCCESS"
            else:
                send_price_status(priceId, "PRICING_IN_PROGRESS", userId)
                status = "IN PROGRESS"
                pricing_data = PricingData.query.get(exec_id)
                pricing_data.pricingStatus = status
                db.session.add(pricing_data)
                db.session.commit()

            if status == "IN PROGRESS":
                ecn_data = pickle.loads(ecn.dataFrame)
                gnt_data = pickle.loads(gnt.dataFrame)
                zco_data = pickle.loads(zco.dataFrame)
                zca_data = pickle.loads(zca.dataFrame)
                cca_data = pickle.loads(cca.dataFrame)
                cfi_data = pickle.loads(cfi.dataFrame)
                solr_data = pickle.loads(solr.dataFrame)
                uni_gas_data = pickle.loads(uni_gas.dataFrame)

                ggl_fixed_data = pickle.loads(ggl_fixed.dataFrame)
                ggl_variable_data = pickle.loads(ggl_variable.dataFrame)
                xo_admin_data = pickle.loads(xo.dataFrame)

                transport_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'transport_ranges.pickle'), 'rb'))
                uni_gas_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'uni_gas_ranges.pickle'), 'rb'))
                timing_risk_data = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'timing_risk.pickle'), 'rb'))

                hedging_cost_data = pickle.loads(hedging_cost.dataFrame)
                hedging_risk_data = pickle.loads(hedging_risk.dataFrame)

                imb_cost_data = pickle.loads(imb_cost.dataFrame)
                imb_risk_data = pickle.loads(imb_risk.dataFrame)

                risk_premium_data = pickle.loads(risk.dataFrame)

                energy_balance_data = pickle.loads(energy_bal.dataFrame)

                gas_trading_data = pickle.loads(gas_trade.dataFrame)

                cts_aq_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'cts_aq_ranges.pickle'), 'rb'))
                cts_mprn_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'cts_mprn_ranges.pickle'), 'rb'))
                cts_dict = pickle.load(open(os.path.join(app.config['PROCESSED_OUT'], 'cts_data.pickle'), 'rb'))

                cobd_cra_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_cra_ranges.pickle'), 'rb'))
                cobd_pb_ranges = pickle.load(
                    open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_pb_ranges.pickle'), 'rb'))
                cobd_dict = pickle.load(open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_data.pickle'), 'rb'))

                copt_data = pickle.loads(copt.dataFrame)

                pre_pricing.remote(mprn_data, unique_mprn_count, price_curve_data_dict, ecn_data, gnt_data, zco_data,
                                   zca_data, cca_data, cfi_data, solr_data, uni_gas_data, transport_ranges,
                                   uni_gas_ranges,
                                   ggl_fixed_data, ggl_variable_data, xo_admin_data, timing_risk_data,
                                   hedging_cost_data,
                                   hedging_risk_data, imb_cost_data, imb_risk_data, risk_premium_data,
                                   energy_balance_data, gas_trading_data, cts_dict, cts_aq_ranges, cts_mprn_ranges,
                                   cobd_cra_ranges, cobd_pb_ranges, cobd_dict, copt_data,
                                   primary_tpi_dict, secondary_tpi_dict, tertiary_tpi_dict,
                                   start_date, end_date, forecast_status.id, exec_id, isHold, userId)

        return jsonify(exec_id)


def update_pricing_status(executionIds):
    if request.method == "GET":
        execution_ids = request.view_args['executionIds']
        status = []
        for i in execution_ids.split(','):
            if not i == "null":
                pricing_status = PricingData.query.get(int(i.replace('\"', '')))
                if pricing_status is not None:
                    status += [i + ':' + pricing_status.pricingStatus]
        return '|'.join(status)


def processEvData(oldMPRNEVJson, updatedEvJSON):
    status = True
    forecastVolume = float(oldMPRNEVJson["Forecast Volume"])
    quotePeriod = int(oldMPRNEVJson["Quote Period"])

    oldUnitRate = oldMPRNEVJson["Unit Rate"]
    newUnitRate = updatedEvJSON["Unit Rate"]
    oldUnitRateMargin = oldMPRNEVJson["UnitRate-Margin"]
    newUnitRateMargin = updatedEvJSON["UnitRate-Margin"]
    try:
        oldUnitMarginPerKwh = oldMPRNEVJson["Unit-Margin-per-kwh"]
    except KeyError:
        oldUnitMarginPerKwh = math.nan
    try:
        newUnitMarginPerKwh = updatedEvJSON["Unit-Margin-per-kwh"]
    except KeyError:
        newUnitMarginPerKwh = math.nan
    oldStandingCharge = oldMPRNEVJson["Standing Charge"]
    newStandingCharge = updatedEvJSON["Standing Charge"]
    oldStandingChargeMargin = oldMPRNEVJson["StandingCharge-Margin"]
    newStandingChargeMargin = updatedEvJSON["StandingCharge-Margin"]

    considerUnitRate = oldUnitRate != newUnitRate and not math.isnan(newUnitRate)
    considerUniteRateMarginPerKWH = (oldUnitMarginPerKwh != newUnitMarginPerKwh) if \
        (not math.isnan(oldUnitMarginPerKwh) and not math.isnan(newUnitMarginPerKwh)) else False
    considerUnitRateMargin = oldUnitRateMargin != newUnitRateMargin and not math.isnan(newUnitRateMargin)
    considerStandingCharge = oldStandingCharge != newStandingCharge and not math.isnan(newStandingCharge)
    considerStandingChargeMargin = \
        oldStandingChargeMargin != newStandingChargeMargin and not math.isnan(newStandingChargeMargin)

    updatedUnitRateMargin = str(oldUnitRateMargin)
    updatedStandingChargeMargin = str(oldStandingChargeMargin)

    if considerUnitRate and not considerUniteRateMarginPerKWH and not considerUnitRateMargin:
        updatedUnitRateMargin = str((newUnitRate * oldMPRNEVJson["Forecast Volume"]) - oldMPRNEVJson["UnitRate-Cost"])
    elif not considerUnitRate and not considerUniteRateMarginPerKWH and considerUnitRateMargin:
        updatedUnitRateMargin = str(newUnitRateMargin)
    elif not considerUnitRate and considerUniteRateMarginPerKWH and not considerUnitRateMargin:
        updatedUnitRateMargin = str((newUnitMarginPerKwh * oldMPRNEVJson["Forecast Volume"]))
    elif not considerUnitRate and not considerUniteRateMarginPerKWH and not considerUnitRateMargin:
        updatedUnitRateMargin = str(oldUnitRateMargin)
    else:
        status = False

    if considerStandingCharge and not considerStandingChargeMargin:
        updatedStandingChargeMargin = \
            str((newStandingCharge * oldMPRNEVJson["Quote Period"]) - oldMPRNEVJson["StandingCharge-Cost"])
    elif not considerStandingCharge and considerStandingChargeMargin:
        updatedStandingChargeMargin = str(newStandingChargeMargin)
    elif not considerStandingCharge and not considerStandingChargeMargin:
        updatedStandingChargeMargin = str(oldStandingChargeMargin)
    else:
        status = False

    unitRateMarginDouble = 0.0
    standingChargeMarginDouble = 0.0

    if not math.isnan(float(updatedUnitRateMargin)) and not math.isnan(forecastVolume) and forecastVolume != 0:
        unitRateMarginDouble = float(updatedUnitRateMargin)
    if not math.isnan(float(updatedStandingChargeMargin)) and quotePeriod != 0:
        standingChargeMarginDouble = float(updatedStandingChargeMargin)

    unitRateRevenue = float(oldMPRNEVJson["UnitRate-Cost"]) + unitRateMarginDouble
    standingChargeRevenue = float(oldMPRNEVJson["StandingCharge-Cost"]) + standingChargeMarginDouble
    totalRevenue = standingChargeRevenue + unitRateRevenue
    totalMargin = standingChargeMarginDouble + unitRateMarginDouble
    unitRate = 0.0
    standingCharge = 0.0
    unitRateMarginPerKWH = 0.0

    if not math.isnan(forecastVolume) and forecastVolume != 0:
        unitRate = unitRateRevenue / forecastVolume
        unitRateMarginPerKWH = unitRateMarginDouble / forecastVolume

    if quotePeriod != 0:
        standingCharge = standingChargeRevenue / quotePeriod

    oldMPRNEVJson["Unit Rate"] = unitRate
    oldMPRNEVJson["UnitRate-Margin"] = unitRateMarginDouble
    oldMPRNEVJson["Unit-Margin-per-kwh"] = unitRateMarginPerKWH
    oldMPRNEVJson["UnitRate-Revenue"] = unitRateRevenue
    oldMPRNEVJson["Standing Charge"] = standingCharge
    oldMPRNEVJson["StandingCharge-Margin"] = standingChargeMarginDouble
    oldMPRNEVJson["StandingCharge-Revenue"] = standingChargeRevenue
    oldMPRNEVJson["Total Margin"] = totalMargin
    oldMPRNEVJson["Total Revenue"] = totalRevenue

    return oldMPRNEVJson, status


def updateMPRNLevelNegotiation():
    if request.method == "POST":
        updatedFile = request.files['file']
        columnNames = request.form.get('visibleCols').split(',')
        pricingId = request.form.get('pricingId')

        excelFile = pd.ExcelFile(updatedFile)

        df_rolling = pd.read_excel(excelFile, pricingId + '_ROLLING')
        df_customer = pd.read_excel(excelFile, pricingId + '_CUSTOMER')

        df_rolling = df_rolling.drop(['MPRN Id', 'Segment Name', 'Group Name'], axis=1)
        df_customer = df_customer.drop(['MPRN Id', 'Segment Name', 'Group Name'], axis=1)

        df_rolling = df_rolling.rename(columns=dict(zip(df_rolling.columns[2:], columnNames)))
        df_customer = df_customer.rename(columns=dict(zip(df_customer.columns[2:], columnNames)))

        key_column = df_rolling.columns[0]
        selected_mprn_column = df_rolling.columns[1]
        value_columns = df_rolling.columns[2:]

        newVersions = []
        selectedEvModelIds = []
        status = True

        for (idx, rollingRow), (_, customerRow) in zip(df_rolling.iterrows(), df_customer.iterrows()):
            pricingSegmentGroupEvaluationModelId = int(rollingRow[key_column])
            selectedMPRN = rollingRow[selected_mprn_column]
            selectedPricingSegmentGroupEvaluationModelVersion = \
                getSelectedPricingSegmentGroupEvaluationModelVersion(pricingSegmentGroupEvaluationModelId)
            evJSON = json.loads(selectedPricingSegmentGroupEvaluationModelVersion.evaluation_json)

            rollingJSON, status1 = processEvData(evJSON[0], rollingRow[value_columns].to_dict())
            customerJSON, status2 = processEvData(evJSON[1], customerRow[value_columns].to_dict())

            if selectedMPRN:
                selectedEvModelIds += [pricingSegmentGroupEvaluationModelId]

            if status1 and status2:
                updatedEvJSON = [rollingJSON, customerJSON]

                all_versions = db.session.query(PricingSegmentGroupEvaluationModelVersionEO).filter(
                    PricingSegmentGroupEvaluationModelVersionEO.pricing_segment_group_evaluation_model_id ==
                    int(pricingSegmentGroupEvaluationModelId))
                db.session.commit()
                all_versions.update({'selected': False})
                db.session.commit()

                newVersions += [
                    PricingSegmentGroupEvaluationModelVersionEO(
                        version=all_versions.count() + 1,
                        evaluation_json=json.dumps(updatedEvJSON, cls=NpJsonEncoder),
                        selected=True,
                        pricing_segment_group_evaluation_model_id=pricingSegmentGroupEvaluationModelId
                    )
                ]
                db.session.commit()
            else:
                status = False

        if status:
            db.session.add_all(newVersions)
            db.session.commit()
            res = {
                "status": "SUCCESS",
                "selectedEvModelIds": selectedEvModelIds
            }
        else:
            res = {
                "status": "NOT_VALID_INPUT",
                "selectedEvModelIds": []
            }
        return jsonify(res)


def getSelectedPricingSegmentGroupEvaluationModelVersion(pricingSegmentGroupEvaluationModelId):
    result = db.session.query(PricingSegmentGroupEvaluationModelVersionEO).filter(
        PricingSegmentGroupEvaluationModelVersionEO.pricing_segment_group_evaluation_model_id ==
        pricingSegmentGroupEvaluationModelId, PricingSegmentGroupEvaluationModelVersionEO.selected == 1).first()
    db.session.commit()
    return result
