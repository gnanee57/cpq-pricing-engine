from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Sequence
from sqlalchemy.ext.declarative import declarative_base

db = SQLAlchemy()

Base = declarative_base()


class PricingData(db.Model):
    __tablename__ = 'r_pricing_data'

    id = db.Column(db.Integer, primary_key=True)
    opportunityId = db.Column(db.String(100), nullable=False)
    priceId = db.Column(db.Integer, nullable=False)
    pricingStartDate = db.Column(db.Date, nullable=True)
    pricingEndDate = db.Column(db.Date, nullable=True)
    pricedDays = db.Column(db.Integer, nullable=True)
    totalMPRNS = db.Column(db.Integer, nullable=True)
    uniqueMPRNS = db.Column(db.Integer, nullable=True)
    pricingStartTime = db.Column(db.DateTime, nullable=True)
    pricingEndTime = db.Column(db.DateTime, nullable=True)
    pricingStatus = db.Column(db.String(100), nullable=True)
    pricingCalcTime = db.Column(db.DateTime, nullable=True)
    pricingCalcDuration = db.Column(db.Float, nullable=True)
    pricingCompletedDuration = db.Column(db.Float, nullable=True)


class ForecastData(db.Model):
    __tablename__ = 'r_forecast_data'

    id = db.Column(db.Integer, primary_key=True)
    opportunityMasterId = db.Column(db.BigInteger, nullable=True)
    opportunityId = db.Column(db.String(100), nullable=False)
    mprnId = db.Column(db.String(100), nullable=False)
    forecastJson = db.Column(db.JSON, nullable=True)
    forecastStartDate = db.Column(db.Date, nullable=True)
    forecastEndDate = db.Column(db.Date, nullable=True)
    forecastDays = db.Column(db.Integer, nullable=True)
    forecastStatus = db.Column(db.String(15), nullable=True)
    forecastRemark = db.Column(db.String(1000), nullable=True)
    totalRollingAq = db.Column(db.Float, nullable=True)
    totalCustomerAq = db.Column(db.Float, nullable=True)
    alpDataUsed = db.Column(db.String(100), nullable=True)
    forecastedAt = db.Column(db.DateTime, nullable=True)
    executionId = db.Column(db.Integer, db.ForeignKey('r_forecast_status.id'), nullable=False)


class ForecastStatus(db.Model):
    __tablename__ = 'r_forecast_status'

    id = db.Column(db.Integer, primary_key=True)
    opportunityMasterId = db.Column(db.BigInteger, nullable=True)
    opportunityId = db.Column(db.String(100), nullable=False)
    forecastStartDate = db.Column(db.Date, nullable=True)
    forecastEndDate = db.Column(db.Date, nullable=True)
    forecastStatus = db.Column(db.String(15), nullable=True)
    totalRollingAq = db.Column(db.Float, nullable=True)
    totalCustomerAq = db.Column(db.Float, nullable=True)
    alpDataUsed = db.Column(db.String(100), nullable=True)
    forecastedAt = db.Column(db.DateTime, nullable=True)
    forecastCalcStartTime = db.Column(db.DateTime, nullable=True)
    forecastCalcEndTime = db.Column(db.DateTime, nullable=True)


class Preprocess(db.Model):
    __tablename__ = 'r_preprocess'

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), nullable=True)
    fileType = db.Column(db.String(100), nullable=True)
    fileCategory = db.Column(db.String(100), nullable=True)
    preprocessedAt = db.Column(db.DateTime, nullable=True)


class PreprocessedData(db.Model):
    __tablename__ = 'r_preprocessed_data'

    id = db.Column(db.Integer, primary_key=True)
    dataFrameType = db.Column(db.String(100), nullable=True)
    dataFrameName = db.Column(db.String(100), nullable=True)
    dataFrame = db.Column(db.PickleType, nullable=False)
    dataFrameStartDate = db.Column(db.Date, nullable=True)
    dataFrameEndDate = db.Column(db.Date, nullable=True)
    createdAt = db.Column(db.DateTime, nullable=True)
    preprocessId = db.Column(db.Integer, db.ForeignKey('r_preprocess.id'), nullable=False)


class PricingSegmentGroupEvaluationModelEO(Base):
    __tablename__ = 'pricing_segment_group_evaluation_modeleo'

    id = db.Column(db.Integer, Sequence('shared_seq'), primary_key=True)
    pricing_id = db.Column(db.Integer, nullable=True)
    opportunity_master_id = db.Column(db.Integer, nullable=True)
    opp_segment_id = db.Column(db.Integer, nullable=True)
    group_id = db.Column(db.Integer)
    mprn_id = db.Column(db.String(255))
    pricing_status = db.Column(db.String(255))
    pricing_remarks = db.Column(db.String(255))
    pricing_segment_group_evaluation_model_versions = \
        db.relationship("PricingSegmentGroupEvaluationModelVersionEO",
                        backref='pricing_segment_group_evaluation_modeleo',
                        lazy=True)
    pricing_json = db.Column(db.String)
    compressed_pricing_json = db.Column(db.BLOB)
    selected = db.Column(db.Boolean, default=False)
    is_compressed = db.Column(db.Boolean, default=False)


class PricingSegmentGroupEvaluationModelVersionEO(Base):
    __tablename__ = 'pricing_segment_group_evaluation_model_versioneo'

    id = db.Column(db.Integer, Sequence('shared_seq'), primary_key=True)
    pricing_segment_group_evaluation_model_id = db.Column(db.Integer,
                                                          db.ForeignKey('pricing_segment_group_evaluation_modeleo.id'),
                                                          nullable=True)
    # pricing_segment_group_evaluation_model = \
    #     db.relationship('PricingSegmentGroupEvaluationModelEO',
    #                     back_populates='pricing_segment_group_evaluation_model_versioneo')
    version = db.Column(db.Integer)
    evaluation_json = db.Column(db.String)
    selected = db.Column(db.Boolean, default=False)
