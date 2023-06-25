from ray import ray
from configure.config import app, scheduler
from pricing.pricing_views import pricing, update_pricing_status, updateMPRNLevelNegotiation
from forecast.forecast_views import forecast, update_forecast_status, get_forecast_report, get_forecast_fail_error
from models.models import db
from preprocess.preprocess_views import preprocess, update_start_end_date
from scheduler.fail_process import fail_app_processes


with app.app_context():
    ray.init()
    db.create_all()

scheduler.add_job(func=fail_app_processes, id='fail_process_scheduler', trigger='interval', seconds=30)
scheduler.start()

app.add_url_rule("/forecast", view_func=forecast, methods=["POST"])
app.add_url_rule("/forecastStatus", view_func=update_forecast_status, methods=["GET"])
app.add_url_rule("/forecastRemark", view_func=get_forecast_report, methods=["GET"])
app.add_url_rule("/forecastFailError", view_func=get_forecast_fail_error, methods=["GET"])
app.add_url_rule("/preprocess", view_func=preprocess, methods=["GET"])
app.add_url_rule("/updateStartAndEndDate", view_func=update_start_end_date, methods=["GET"])
app.add_url_rule("/pricing", view_func=pricing, methods=["POST"])
app.add_url_rule("/updatePricingStatus/<executionIds>", view_func=update_pricing_status, methods=["GET"])
app.add_url_rule("/updateMPRNLevelNegotiation", view_func=updateMPRNLevelNegotiation, methods=["POST"])


if __name__ == "__main__":
    app.run()
