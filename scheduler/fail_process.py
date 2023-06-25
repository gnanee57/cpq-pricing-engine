import datetime

from sqlalchemy import text

from configure.config import app
from models.models import ForecastStatus, db, PricingData


def fail_app_processes():
    two_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=2)
    with app.app_context():
        failed_forecasts = ForecastStatus.query.filter(ForecastStatus.forecastStatus == "In Progress",
                                                       ForecastStatus.forecastCalcStartTime <= two_hours_ago).all()
        if failed_forecasts is not None:
            for i in range(len(failed_forecasts)):
                failed_forecast = failed_forecasts[i]
                failed_forecast.forecastStatus = "FAILED"
                failed_forecast.forecastCalcEndTime = datetime.datetime.now()
                failed_forecasts[i] = failed_forecast

                update_opportunity_status = \
                    text("UPDATE `opportunity_mastereo` SET `forecast_status`=:status WHERE id=:opportunityMasterId")
                db.session.execute(update_opportunity_status, {'status': "FAILED",
                                                               'opportunityMasterId':
                                                                   failed_forecast.opportunityMasterId})
                db.session.commit()
                update_raengine_log = \
                    text("UPDATE `raengine_logeo` SET `execution_status`=:status WHERE `iras_execution_id`=:executionId")
                db.session.execute(update_raengine_log, {'status': "FAILED",
                                                         'executionId': failed_forecast.id})
                db.session.commit()
            db.session.add_all(failed_forecasts)
            db.session.commit()

        failed_pricings = PricingData.query.filter(PricingData.pricingStatus == "In Progress",
                                                   PricingData.pricingStartTime <= two_hours_ago).all()
        if failed_pricings is not None:
            for i in range(len(failed_pricings)):
                failed_pricing = failed_pricings[i]
                failed_pricing.pricingStatus = "FAILED"
                failed_pricing.pricingEndTime = datetime.datetime.now()
                failed_pricings[i] = failed_pricing

                update_eval_status_query = \
                    text('UPDATE `pricing_mastereo` SET '
                         '`pricing_evaluation_status`=:evalStatus , `calculating`=:status, '
                         '`last_priced_date_time`=:pricedTime WHERE `pricing_id`=:pricingId')

                db.session.execute(update_eval_status_query, {'evalStatus': None,
                                                              'status': "FAILED",
                                                              'pricedTime': datetime.datetime.now(),
                                                              'pricingId': failed_pricing.priceId
                                                              })
                db.session.commit()
                ra_engine_log_update_query = \
                    text('UPDATE `raengine_logeo` SET `execution_status`=:priceStatus, '
                         '`process_end_time`=:currTime WHERE `iras_execution_id`=:execId')
                db.session.execute(ra_engine_log_update_query,
                                   {'priceStatus': "FAILED",
                                    'currTime': datetime.datetime.now(),
                                    'execId': failed_pricing.priceId})
                db.session.commit()
            db.session.add_all(failed_pricings)
            db.session.commit()
