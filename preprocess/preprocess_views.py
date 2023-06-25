import datetime
import os

from flask import request, jsonify
from sqlalchemy import text

from configure.config import app
from exceptions.preprocess_exceptions import DelimiterError, ColExtraFoundError, ColNotFoundError, ColLessFoundError, \
    DataDuplicateError, DataNotFoundError, RowEmptyError, DataDuplicateError1, DataDuplicateError2, ValueNegativeError
from models.models import Preprocess, PreprocessedData, db
from preprocess.preprocess import preprocess_alp, preprocess_transportation, preprocess_unigas, preprocess_ggl_fixed, \
    preprocess_ggl_variable, preprocess_xo_admin, preprocess_timing_risk, preprocess_hedging, preprocess_cts, \
    preprocess_cobd, preprocess_copt, preprocess_forecast_input_band_range, preprocess_tpi_limits, \
    preprocess_load_factor, get_preprocessed_file


def preprocess():
    try:
        file_name = request.args.get('filename')
        file_type = request.args.get('fileType')
        file_category = request.args.get('fileCategory')

        file_path = os.path.join(app.config['PREPROCESS_DIR'], file_category, file_name)

        pickle_files = {}
        startDate = None
        endDate = None

        if file_category == 'ALP':
            pickle_files, startDate, endDate = preprocess_alp(file_path)

        if file_category == 'TRANSPORTATION':
            pickle_files, startDate, endDate = preprocess_transportation(file_path)

        if file_category == 'UNI_GAS':
            pickle_files, startDate, endDate = preprocess_unigas(file_path)

        if file_category == 'GGL_Fixed':
            pickle_files, startDate, endDate = preprocess_ggl_fixed(file_path)

        if file_category == 'GGL_Variable':
            pickle_files, startDate, endDate = preprocess_ggl_variable(file_path)

        if file_category == 'XO_ADMIN':
            pickle_files, startDate, endDate = preprocess_xo_admin(file_path)

        if file_category == 'Timing_Risk':
            pickle_files = preprocess_timing_risk(file_path)

        if file_category == 'Hedging_Cost':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'Timing_Risk_Monthly':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'Hedging_Risk':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'IMB_Cost':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'IMB_Risk':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'RiskPremium':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'EnergyBalance':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'Gas_Trade':
            pickle_files, startDate, endDate = preprocess_hedging(file_path, file_category)

        if file_category == 'CTS':
            pickle_files = preprocess_cts(file_path, file_category)

        if file_category == 'COBD':
            pickle_files = preprocess_cobd(file_path, file_category)

        if file_category == 'COPT':
            pickle_files, startDate, endDate = preprocess_copt(file_path, file_category)

        if file_category == 'INPUT_BANDS':
            pickle_files = preprocess_forecast_input_band_range(file_path, file_category)

        if file_category == 'TPI_Limits':
            pickle_files = preprocess_tpi_limits(file_path, file_category)

        if file_category == 'LOAD_FACTOR':
            pickle_files = preprocess_load_factor(file_path, file_category)

        _preprocess = Preprocess(filename=file_name, fileType=file_type, fileCategory=file_category,
                                 preprocessedAt=datetime.datetime.now())
        db.session.add(_preprocess)
        db.session.commit()

        _preprocess_data = []
        for i in pickle_files.keys():
            dataFrameType = i
            pickle_file = pickle_files[i]
            _preprocess_data += [PreprocessedData(dataFrameName=file_name, dataFrameType=dataFrameType,
                                                  dataFrame=pickle_file, createdAt=datetime.datetime.now(),
                                                  preprocessId=_preprocess.id, dataFrameStartDate=startDate,
                                                  dataFrameEndDate=endDate)]

        db.session.bulk_save_objects(_preprocess_data)
        db.session.commit()

        return jsonify('SUCCESS')
    except (DelimiterError, ColLessFoundError, ColExtraFoundError, ColNotFoundError,
            DataDuplicateError, DataNotFoundError, RowEmptyError, DataDuplicateError1, DataDuplicateError2,
            ValueNegativeError) as e:
        print("Exception", e)
        return jsonify(e.error_code)


def update_start_end_date():
    alpId = request.args.get('alpId')
    processed_alp = get_preprocessed_file('ALP')
    if processed_alp is not None:
        start_date = processed_alp.dataFrameStartDate
        end_date = processed_alp.dataFrameEndDate

        update_alp_query = text("UPDATE `alp_eo` SET `start_date`=:start_date, `end_date`=:end_date WHERE id=:alpId")

        db.session.execute(update_alp_query, {'start_date': start_date,
                                              'end_date': end_date,
                                              'alpId': alpId
                                              })
        db.session.commit()

        return jsonify('SUCCESS')
    else:
        return jsonify('FAILED')
