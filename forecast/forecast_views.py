import json
import os
import pickle
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
from flask import request, jsonify

from configure.config import app
from forecast.forecast import pre_forecast
from models.models import ForecastStatus, db, ForecastData
from preprocess.preprocess import get_preprocessed_file


def forecast():
    if request.method == "POST":
        payload = json.loads(request.data.decode("utf-8"))
        mprn_csv = payload["reconName"]
        mprn_csv_io = StringIO(mprn_csv)
        start_date = datetime.strptime(payload["startDate"], '%Y-%m-%d')
        end_date = datetime.strptime(payload["endDate"], '%Y-%m-%d')

        mprn_df = pd.read_csv(mprn_csv_io, sep='|', lineterminator='~', index_col=False,
                              converters={'Mprn Id': str, 'Opportunity Id': str,
                                          'End Usr Cat Code': str, 'Annual Quantity': str,
                                          'Customer AQ': str})

        mprn_df['isDuplicate'] = np.where(mprn_df.duplicated(subset=['Mprn Id'], keep=False), True, False)
        mprn_df = mprn_df.drop_duplicates(subset='Mprn Id', keep='first')
        mprn_data = mprn_df.to_dict('records')

        alp_data = get_preprocessed_file("ALP")
        processed_alp = pickle.loads(alp_data.dataFrame)
        alpDataUsed = alp_data.dataFrameName
        forcast_band_ranges = pickle.load(open(os.path.join(app.config['PROCESSED_OUT'], 'forcast_band_ranges.pickle'), 'rb'))

        forecast_status = ForecastStatus(
            opportunityMasterId=mprn_data[0]['Opportunity Master Id'],
            opportunityId=mprn_data[0]['Opportunity Id'],
            forecastStatus="In Progress",
            forecastStartDate=start_date.date(),
            forecastEndDate=end_date.date(),
            totalRollingAq=None,
            totalCustomerAq=None,
            alpDataUsed=alpDataUsed,
            forecastedAt=None,
            forecastCalcStartTime=datetime.now()
        )

        db.session.add(forecast_status)
        db.session.commit()
        exec_id = forecast_status.id

        with open(os.path.join(app.config['FORECAST_INPUT'], str(exec_id) + '_' + str(mprn_data[0]['Opportunity Id']) + '.csv'), 'w') as out:
            out.write(mprn_csv.replace("~", "\n"))

        os.makedirs(os.path.join(app.config['FORECAST_OUT'], str(exec_id)))

        # inspect_serializability(pre_forecast.remote)
        pre_forecast.remote(mprn_data, processed_alp, start_date, end_date, exec_id,
                            alpDataUsed, forcast_band_ranges)
        return jsonify(exec_id)


def update_forecast_status():
    if request.method == "GET":
        execution_id = request.args.get('executionId')
        forecast_status = ForecastStatus.query.get(execution_id)
        if forecast_status is not None:
            return forecast_status.forecastStatus
        return jsonify(None)


def get_forecast_report():
    if request.method == "GET":
        execution_id = request.args.get('executionId')
        forecast_data = ForecastData.query.filter(ForecastData.executionId == execution_id)
        if forecast_data is not None:
            mprnList = [i.mprnId for i in forecast_data if i.forecastRemark is not None and
                        i.forecastRemark != ""]
            uniqueMprns = set(mprnList)
            if len(mprnList) == len(uniqueMprns):
                return jsonify('--'.join([i.mprnId + '~' + i.forecastRemark for i in forecast_data if
                                          i.forecastRemark is not None and i.forecastRemark != ""]))
            else:
                remarks = []
                for i in uniqueMprns:
                    count = 0
                    remark = ''
                    remark_types = set()
                    for j in forecast_data:
                        if j.forecastRemark is not None and j.forecastRemark != "" and j.mprnId == i:
                            if count == 0:
                                remark_type = 'Failed' if 'Failed' in j.forecastRemark else 'Warning'
                                remark_types.add(remark_type)
                                replaced_remark = j.forecastRemark.replace(remark_type + ': ', '')
                                remark += replaced_remark
                                count += 1
                            else:
                                remark_type = 'Failed' if 'Failed' in j.forecastRemark else 'Warning'
                                remark_types.add(remark_type)
                                replaced_remark = j.forecastRemark.replace(remark_type + ': ', '')
                                remark += ' ; ' + replaced_remark
                                count += 1
                    remarks += [
                        i + '~' + ('Failed: ' if 'Failed' in remark_types else 'Warning: ') + remark +
                        ('(Duplicate MPRN)' if count > 1 else '')
                    ]
                return jsonify('--'.join(remarks))
        return jsonify(None)


def get_mprn_forecast(mprnId, oppId, executionId):
    return ForecastData.query.filter(ForecastData.mprnId == mprnId,
                                     ForecastData.opportunityMasterId == oppId,
                                     ForecastData.executionId == executionId).first()


def get_latest_forecast_status(oppId):
    return ForecastStatus.query.filter(ForecastStatus.opportunityMasterId == oppId) \
        .order_by(ForecastStatus.id.desc()).first()


def get_forecast_fail_error():
    if request.method == "GET":
        opportunity_id = request.args.get('opportunityId')
        mprn_id = request.args.get('mprnId')
        execution_id = request.args.get('executionId')
        forecast_data = ForecastData.query.filter(ForecastData.executionId == int(execution_id),
                                                  ForecastData.opportunityId.like(opportunity_id),
                                                  ForecastData.mprnId.like(mprn_id)).first()
        if forecast_data is not None:
            return jsonify(forecast_data.forecastRemark)
        else:
            return jsonify("")
