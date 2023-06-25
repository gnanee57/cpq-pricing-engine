import json
import os

import paramiko
import requests
import zlib

from sqlalchemy import text

from configure.config import app
from models.models import db


def sendPricingData(exec_id, payload, pricingId):
    # response = requests.post('http://localhost:8080/pricing/evaluation', json=payload)
    # if not response.ok:
    #     with open('static/out/pricing_out/' + str(exec_id) + '.json', 'w') as out:
    #         json.dump(payload, out)
    # print(response)
    # return response.ok
    try:
        with app.app_context():
            delete_old_evaluation_query = \
                text('DELETE FROM `pricing_segment_group_evaluation_modeleo` WHERE pricing_id=:pricingId')
            db.session.execute(delete_old_evaluation_query, {'pricingId': pricingId})
            db.session.commit()
            for i in range(0, len(payload), 500):
                db.session.add_all(payload[i:i+500])
                db.session.commit()
        return True

    except Exception as e:
        print(e)
        json_out = []
        for i in payload:
            json_out += [{
                    "pricingMasterId": i.pricing_id,
                    "opportunityMasterId": i.opportunity_master_id,
                    "pricingOpportunitySegmentMasterId": i.opp_segment_id,
                    "segmentId": i.opp_segment_id,
                    "groupId": i.group_id,
                    "mprnId": i.mprn_id,
                    "pricingStatus": i.pricing_status,
                    "pricingRemarks": i.pricing_remarks,
                    "isHoldPrice": "N",
                    "pricingJson": zlib.decompress(i.pricing_json),
                    "pricingEvaluationModelVersions": [{
                        "evaluationJson": i.pricing_segment_group_evaluation_model_versions[0].evaluation_json,
                        "selected": 'true'
                    }]
            }]
        with app.app_context():
            with open(os.path.join(app.config['PRICING_OUT'], str(exec_id) + '.json', 'w')) as out:
                json.dump(json_out, out)
        return False


def send_price_status(pricingId, status, userId):
    session = requests.session()
    session.cookies.set(name="authToken", value=app.config.get('CPQ_AUTH_TOKEN'))
    response = session.get(
        app.config.get('CPQ_URL') + '/pricing/' + str(pricingId) + '/updatePricingStatus?status=' + status + '&userId=' + userId
    )
    session.close()
    print(response)


def send_for_hold_price_evaluation(pricingId, userId):
    session = requests.session()
    session.cookies.set(name="authToken", value=app.config.get('CPQ_AUTH_TOKEN'))
    response = session.get(app.config.get('CPQ_URL') + '/pricing/' + str(pricingId) + '/createPricingEvaluationHoldPrice?userId=' + userId)
    session.close()
    print(response)


def send_customer_soq_calc(opportunityMasterId):
    session = requests.session()
    session.cookies.set(name="authToken", value=app.config.get('CPQ_AUTH_TOKEN'))
    response = session.get(
        url=app.config.get('CPQ_URL') + '/opportunity/' + str(opportunityMasterId) + '/updateCustSOQProfileAndGroup'
    )
    session.close()
    print(response)


def transfer_file(localPath, remotePath):
    with paramiko.SSHClient() as ssh:
        ssh.load_system_host_keys()
        ssh.connect(
            hostname=app.config.get('cpq_host'),
            username=app.config.get('cpq_user_name'),
            password=app.config.get('cpq_password')
        )
        sftp = ssh.open_sftp()
        sftp.put(localPath, remotePath)
