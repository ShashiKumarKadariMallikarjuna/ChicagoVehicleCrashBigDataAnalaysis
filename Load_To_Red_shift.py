import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701491573787 = glueContext.create_dynamic_frame.from_catalog(
    database="crawler_database",
    table_name="people",
    transformation_ctx="AWSGlueDataCatalog_node1701491573787",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701491574525 = glueContext.create_dynamic_frame.from_catalog(
    database="crawler_database",
    table_name="vehicles",
    transformation_ctx="AWSGlueDataCatalog_node1701491574525",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701491572970 = glueContext.create_dynamic_frame.from_catalog(
    database="crawler_database",
    table_name="crashes",
    transformation_ctx="AWSGlueDataCatalog_node1701491572970",
)

# Script generated for node Change Schema
ChangeSchema_node1701491583087 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1701491573787,
    mappings=[
        ("person_id", "string", "person_id", "string"),
        ("person_type", "string", "person_type", "string"),
        ("crash_record_id", "string", "crash_record_id", "string"),
        ("vehicle_id", "double", "vehicle_id", "double"),
        ("seat_no", "double", "seat_no", "double"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("zipcode", "long", "zipcode", "long"),
        ("sex", "string", "sex", "string"),
        ("age", "double", "age", "double"),
        ("drivers_license_state", "string", "drivers_license_state", "string"),
        ("drivers_license_class", "string", "drivers_license_class", "string"),
        ("safety_equipment", "string", "safety_equipment", "string"),
        ("airbag_deployed", "string", "airbag_deployed", "string"),
        ("ejection", "string", "ejection", "string"),
        ("injury_classification", "string", "injury_classification", "string"),
        ("hospital", "string", "hospital", "string"),
        ("ems_agency", "string", "ems_agency", "string"),
        ("ems_run_no", "string", "ems_run_no", "string"),
        ("driver_action", "string", "driver_action", "string"),
        ("driver_vision", "string", "driver_vision", "string"),
        ("physical_condition", "string", "physical_condition", "string"),
        ("pedpedal_action", "string", "pedpedal_action", "string"),
        ("pedpedal_visibility", "string", "pedpedal_visibility", "string"),
        ("pedpedal_location", "string", "pedpedal_location", "string"),
        ("bac_result", "string", "bac_result", "string"),
        ("bac_result value", "double", "bac_result value", "double"),
        ("cell_phone_use", "string", "cell_phone_use", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701491583087",
)

# Script generated for node Change Schema
ChangeSchema_node1701491579078 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1701491574525,
    mappings=[
        ("crash_unit_id", "long", "crash_unit_id", "long"),
        ("crash_record_id", "string", "crash_record_id", "string"),
        ("unit_no", "long", "unit_no", "long"),
        ("unit_type", "string", "unit_type", "string"),
        ("num_passengers", "double", "num_passengers", "double"),
        ("vehicle_id", "double", "vehicle_id", "double"),
        ("make", "string", "make", "string"),
        ("model", "string", "model", "string"),
        ("lic_plate_state", "string", "lic_plate_state", "string"),
        ("vehicle_year", "string", "vehicle_year", "string"),
        ("vehicle_defect", "string", "vehicle_defect", "string"),
        ("vehicle_type", "string", "vehicle_type", "string"),
        ("vehicle_use", "string", "vehicle_use", "string"),
        ("travel_direction", "string", "travel_direction", "string"),
        ("maneuver", "string", "maneuver", "string"),
        ("towed_i", "string", "towed_i", "string"),
        ("occupant_cnt", "long", "occupant_cnt", "long"),
        ("exceed_speed_limit_i", "string", "exceed_speed_limit_i", "string"),
        ("towed_by", "string", "towed_by", "string"),
        ("towed_to", "string", "towed_to", "string"),
        ("first_contact_point", "string", "first_contact_point", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701491579078",
)

# Script generated for node Change Schema
ChangeSchema_node1701491588946 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1701491572970,
    mappings=[
        ("crash_record_id", "string", "crash_record_id", "string"),
        ("crash_date", "string", "crash_date", "string"),
        ("posted_speed_limit", "long", "posted_speed_limit", "long"),
        ("traffic_control_device", "string", "traffic_control_device", "string"),
        ("device_condition", "string", "device_condition", "string"),
        ("weather_condition", "string", "weather_condition", "string"),
        ("lighting_condition", "string", "lighting_condition", "string"),
        ("first_crash_type", "string", "first_crash_type", "string"),
        ("trafficway_type", "string", "trafficway_type", "string"),
        ("lane_cnt", "double", "lane_cnt", "double"),
        ("alignment", "string", "alignment", "string"),
        ("roadway_surface_cond", "string", "roadway_surface_cond", "string"),
        ("road_defect", "string", "road_defect", "string"),
        ("report_type", "string", "report_type", "string"),
        ("crash_type", "string", "crash_type", "string"),
        ("intersection_related_i", "string", "intersection_related_i", "string"),
        ("not_right_of_way_i", "string", "not_right_of_way_i", "string"),
        ("hit_and_run_i", "string", "hit_and_run_i", "string"),
        ("damage", "string", "damage", "string"),
        ("date_police_notified", "string", "date_police_notified", "string"),
        ("prim_contributory_cause", "string", "prim_contributory_cause", "string"),
        ("sec_contributory_cause", "string", "sec_contributory_cause", "string"),
        ("street_no", "long", "street_no", "long"),
        ("street_direction", "string", "street_direction", "string"),
        ("street_name", "string", "street_name", "string"),
        ("beat_of_occurrence", "double", "beat_of_occurrence", "double"),
        ("photos_taken_i", "string", "photos_taken_i", "string"),
        ("statements_taken_i", "string", "statements_taken_i", "string"),
        ("dooring_i", "string", "dooring_i", "string"),
        ("work_zone_i", "string", "work_zone_i", "string"),
        ("work_zone_type", "string", "work_zone_type", "string"),
        ("workers_present_i", "string", "workers_present_i", "string"),
        ("num_units", "long", "num_units", "long"),
        ("most_severe_injury", "string", "most_severe_injury", "string"),
        ("injuries_total", "double", "injuries_total", "double"),
        ("injuries_fatal", "double", "injuries_fatal", "double"),
        ("injuries_incapacitating", "double", "injuries_incapacitating", "double"),
        (
            "injuries_non_incapacitating",
            "double",
            "injuries_non_incapacitating",
            "double",
        ),
        (
            "injuries_reported_not_evident",
            "double",
            "injuries_reported_not_evident",
            "double",
        ),
        ("injuries_no_indication", "double", "injuries_no_indication", "double"),
        ("injuries_unknown", "double", "injuries_unknown", "double"),
        ("crash_hour", "long", "crash_hour", "long"),
        ("crash_day_of_week", "long", "crash_day_of_week", "long"),
        ("crash_month", "long", "crash_month", "long"),
        ("latitude", "double", "latitude", "double"),
        ("longitude", "double", "longitude", "double"),
    ],
    transformation_ctx="ChangeSchema_node1701491588946",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1701491598124 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1701491583087,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.people USING public.people_temp_551a57 ON people.vehicle_id = people_temp_551a57.vehicle_id WHEN MATCHED THEN UPDATE SET person_id = people_temp_551a57.person_id, person_type = people_temp_551a57.person_type, crash_record_id = people_temp_551a57.crash_record_id, vehicle_id = people_temp_551a57.vehicle_id, seat_no = people_temp_551a57.seat_no, city = people_temp_551a57.city, state = people_temp_551a57.state, zipcode = people_temp_551a57.zipcode, sex = people_temp_551a57.sex, age = people_temp_551a57.age, drivers_license_state = people_temp_551a57.drivers_license_state, drivers_license_class = people_temp_551a57.drivers_license_class, safety_equipment = people_temp_551a57.safety_equipment, airbag_deployed = people_temp_551a57.airbag_deployed, ejection = people_temp_551a57.ejection, injury_classification = people_temp_551a57.injury_classification, hospital = people_temp_551a57.hospital, ems_agency = people_temp_551a57.ems_agency, ems_run_no = people_temp_551a57.ems_run_no, driver_action = people_temp_551a57.driver_action, driver_vision = people_temp_551a57.driver_vision, physical_condition = people_temp_551a57.physical_condition, pedpedal_action = people_temp_551a57.pedpedal_action, pedpedal_visibility = people_temp_551a57.pedpedal_visibility, pedpedal_location = people_temp_551a57.pedpedal_location, bac_result = people_temp_551a57.bac_result, bac_result value = people_temp_551a57.bac_result value, cell_phone_use = people_temp_551a57.cell_phone_use WHEN NOT MATCHED THEN INSERT VALUES (people_temp_551a57.person_id, people_temp_551a57.person_type, people_temp_551a57.crash_record_id, people_temp_551a57.vehicle_id, people_temp_551a57.seat_no, people_temp_551a57.city, people_temp_551a57.state, people_temp_551a57.zipcode, people_temp_551a57.sex, people_temp_551a57.age, people_temp_551a57.drivers_license_state, people_temp_551a57.drivers_license_class, people_temp_551a57.safety_equipment, people_temp_551a57.airbag_deployed, people_temp_551a57.ejection, people_temp_551a57.injury_classification, people_temp_551a57.hospital, people_temp_551a57.ems_agency, people_temp_551a57.ems_run_no, people_temp_551a57.driver_action, people_temp_551a57.driver_vision, people_temp_551a57.physical_condition, people_temp_551a57.pedpedal_action, people_temp_551a57.pedpedal_visibility, people_temp_551a57.pedpedal_location, people_temp_551a57.bac_result, people_temp_551a57.bac_result value, people_temp_551a57.cell_phone_use); DROP TABLE public.people_temp_551a57; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-369329160416-us-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.people_temp_551a57",
        "connectionName": "redshift_connector",
        "preactions": "CREATE TABLE IF NOT EXISTS public.people (person_id VARCHAR, person_type VARCHAR, crash_record_id VARCHAR, vehicle_id DOUBLE PRECISION, seat_no DOUBLE PRECISION, city VARCHAR, state VARCHAR, zipcode BIGINT, sex VARCHAR, age DOUBLE PRECISION, drivers_license_state VARCHAR, drivers_license_class VARCHAR, safety_equipment VARCHAR, airbag_deployed VARCHAR, ejection VARCHAR, injury_classification VARCHAR, hospital VARCHAR, ems_agency VARCHAR, ems_run_no VARCHAR, driver_action VARCHAR, driver_vision VARCHAR, physical_condition VARCHAR, pedpedal_action VARCHAR, pedpedal_visibility VARCHAR, pedpedal_location VARCHAR, bac_result VARCHAR, bac_result value DOUBLE PRECISION, cell_phone_use VARCHAR); DROP TABLE IF EXISTS public.people_temp_551a57; CREATE TABLE public.people_temp_551a57 (person_id VARCHAR, person_type VARCHAR, crash_record_id VARCHAR, vehicle_id DOUBLE PRECISION, seat_no DOUBLE PRECISION, city VARCHAR, state VARCHAR, zipcode BIGINT, sex VARCHAR, age DOUBLE PRECISION, drivers_license_state VARCHAR, drivers_license_class VARCHAR, safety_equipment VARCHAR, airbag_deployed VARCHAR, ejection VARCHAR, injury_classification VARCHAR, hospital VARCHAR, ems_agency VARCHAR, ems_run_no VARCHAR, driver_action VARCHAR, driver_vision VARCHAR, physical_condition VARCHAR, pedpedal_action VARCHAR, pedpedal_visibility VARCHAR, pedpedal_location VARCHAR, bac_result VARCHAR, bac_result value DOUBLE PRECISION, cell_phone_use VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1701491598124",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1701491604489 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1701491579078,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.vehicles USING public.vehicles_temp_ea85d0 ON vehicles.vehicle_id = vehicles_temp_ea85d0.vehicle_id WHEN MATCHED THEN UPDATE SET crash_unit_id = vehicles_temp_ea85d0.crash_unit_id, crash_record_id = vehicles_temp_ea85d0.crash_record_id, unit_no = vehicles_temp_ea85d0.unit_no, unit_type = vehicles_temp_ea85d0.unit_type, num_passengers = vehicles_temp_ea85d0.num_passengers, vehicle_id = vehicles_temp_ea85d0.vehicle_id, make = vehicles_temp_ea85d0.make, model = vehicles_temp_ea85d0.model, lic_plate_state = vehicles_temp_ea85d0.lic_plate_state, vehicle_year = vehicles_temp_ea85d0.vehicle_year, vehicle_defect = vehicles_temp_ea85d0.vehicle_defect, vehicle_type = vehicles_temp_ea85d0.vehicle_type, vehicle_use = vehicles_temp_ea85d0.vehicle_use, travel_direction = vehicles_temp_ea85d0.travel_direction, maneuver = vehicles_temp_ea85d0.maneuver, towed_i = vehicles_temp_ea85d0.towed_i, occupant_cnt = vehicles_temp_ea85d0.occupant_cnt, exceed_speed_limit_i = vehicles_temp_ea85d0.exceed_speed_limit_i, towed_by = vehicles_temp_ea85d0.towed_by, towed_to = vehicles_temp_ea85d0.towed_to, first_contact_point = vehicles_temp_ea85d0.first_contact_point WHEN NOT MATCHED THEN INSERT VALUES (vehicles_temp_ea85d0.crash_unit_id, vehicles_temp_ea85d0.crash_record_id, vehicles_temp_ea85d0.unit_no, vehicles_temp_ea85d0.unit_type, vehicles_temp_ea85d0.num_passengers, vehicles_temp_ea85d0.vehicle_id, vehicles_temp_ea85d0.make, vehicles_temp_ea85d0.model, vehicles_temp_ea85d0.lic_plate_state, vehicles_temp_ea85d0.vehicle_year, vehicles_temp_ea85d0.vehicle_defect, vehicles_temp_ea85d0.vehicle_type, vehicles_temp_ea85d0.vehicle_use, vehicles_temp_ea85d0.travel_direction, vehicles_temp_ea85d0.maneuver, vehicles_temp_ea85d0.towed_i, vehicles_temp_ea85d0.occupant_cnt, vehicles_temp_ea85d0.exceed_speed_limit_i, vehicles_temp_ea85d0.towed_by, vehicles_temp_ea85d0.towed_to, vehicles_temp_ea85d0.first_contact_point); DROP TABLE public.vehicles_temp_ea85d0; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-369329160416-us-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.vehicles_temp_ea85d0",
        "connectionName": "redshift_connector",
        "preactions": "CREATE TABLE IF NOT EXISTS public.vehicles (crash_unit_id BIGINT, crash_record_id VARCHAR, unit_no BIGINT, unit_type VARCHAR, num_passengers DOUBLE PRECISION, vehicle_id DOUBLE PRECISION, make VARCHAR, model VARCHAR, lic_plate_state VARCHAR, vehicle_year VARCHAR, vehicle_defect VARCHAR, vehicle_type VARCHAR, vehicle_use VARCHAR, travel_direction VARCHAR, maneuver VARCHAR, towed_i VARCHAR, occupant_cnt BIGINT, exceed_speed_limit_i VARCHAR, towed_by VARCHAR, towed_to VARCHAR, first_contact_point VARCHAR); DROP TABLE IF EXISTS public.vehicles_temp_ea85d0; CREATE TABLE public.vehicles_temp_ea85d0 (crash_unit_id BIGINT, crash_record_id VARCHAR, unit_no BIGINT, unit_type VARCHAR, num_passengers DOUBLE PRECISION, vehicle_id DOUBLE PRECISION, make VARCHAR, model VARCHAR, lic_plate_state VARCHAR, vehicle_year VARCHAR, vehicle_defect VARCHAR, vehicle_type VARCHAR, vehicle_use VARCHAR, travel_direction VARCHAR, maneuver VARCHAR, towed_i VARCHAR, occupant_cnt BIGINT, exceed_speed_limit_i VARCHAR, towed_by VARCHAR, towed_to VARCHAR, first_contact_point VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1701491604489",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1701491591925 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1701491588946,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.crashes USING public.crashes_temp_3a1dee ON crashes.crash_record_id = crashes_temp_3a1dee.crash_record_id WHEN MATCHED THEN UPDATE SET crash_record_id = crashes_temp_3a1dee.crash_record_id, crash_date = crashes_temp_3a1dee.crash_date, posted_speed_limit = crashes_temp_3a1dee.posted_speed_limit, traffic_control_device = crashes_temp_3a1dee.traffic_control_device, device_condition = crashes_temp_3a1dee.device_condition, weather_condition = crashes_temp_3a1dee.weather_condition, lighting_condition = crashes_temp_3a1dee.lighting_condition, first_crash_type = crashes_temp_3a1dee.first_crash_type, trafficway_type = crashes_temp_3a1dee.trafficway_type, lane_cnt = crashes_temp_3a1dee.lane_cnt, alignment = crashes_temp_3a1dee.alignment, roadway_surface_cond = crashes_temp_3a1dee.roadway_surface_cond, road_defect = crashes_temp_3a1dee.road_defect, report_type = crashes_temp_3a1dee.report_type, crash_type = crashes_temp_3a1dee.crash_type, intersection_related_i = crashes_temp_3a1dee.intersection_related_i, not_right_of_way_i = crashes_temp_3a1dee.not_right_of_way_i, hit_and_run_i = crashes_temp_3a1dee.hit_and_run_i, damage = crashes_temp_3a1dee.damage, date_police_notified = crashes_temp_3a1dee.date_police_notified, prim_contributory_cause = crashes_temp_3a1dee.prim_contributory_cause, sec_contributory_cause = crashes_temp_3a1dee.sec_contributory_cause, street_no = crashes_temp_3a1dee.street_no, street_direction = crashes_temp_3a1dee.street_direction, street_name = crashes_temp_3a1dee.street_name, beat_of_occurrence = crashes_temp_3a1dee.beat_of_occurrence, photos_taken_i = crashes_temp_3a1dee.photos_taken_i, statements_taken_i = crashes_temp_3a1dee.statements_taken_i, dooring_i = crashes_temp_3a1dee.dooring_i, work_zone_i = crashes_temp_3a1dee.work_zone_i, work_zone_type = crashes_temp_3a1dee.work_zone_type, workers_present_i = crashes_temp_3a1dee.workers_present_i, num_units = crashes_temp_3a1dee.num_units, most_severe_injury = crashes_temp_3a1dee.most_severe_injury, injuries_total = crashes_temp_3a1dee.injuries_total, injuries_fatal = crashes_temp_3a1dee.injuries_fatal, injuries_incapacitating = crashes_temp_3a1dee.injuries_incapacitating, injuries_non_incapacitating = crashes_temp_3a1dee.injuries_non_incapacitating, injuries_reported_not_evident = crashes_temp_3a1dee.injuries_reported_not_evident, injuries_no_indication = crashes_temp_3a1dee.injuries_no_indication, injuries_unknown = crashes_temp_3a1dee.injuries_unknown, crash_hour = crashes_temp_3a1dee.crash_hour, crash_day_of_week = crashes_temp_3a1dee.crash_day_of_week, crash_month = crashes_temp_3a1dee.crash_month, latitude = crashes_temp_3a1dee.latitude, longitude = crashes_temp_3a1dee.longitude WHEN NOT MATCHED THEN INSERT VALUES (crashes_temp_3a1dee.crash_record_id, crashes_temp_3a1dee.crash_date, crashes_temp_3a1dee.posted_speed_limit, crashes_temp_3a1dee.traffic_control_device, crashes_temp_3a1dee.device_condition, crashes_temp_3a1dee.weather_condition, crashes_temp_3a1dee.lighting_condition, crashes_temp_3a1dee.first_crash_type, crashes_temp_3a1dee.trafficway_type, crashes_temp_3a1dee.lane_cnt, crashes_temp_3a1dee.alignment, crashes_temp_3a1dee.roadway_surface_cond, crashes_temp_3a1dee.road_defect, crashes_temp_3a1dee.report_type, crashes_temp_3a1dee.crash_type, crashes_temp_3a1dee.intersection_related_i, crashes_temp_3a1dee.not_right_of_way_i, crashes_temp_3a1dee.hit_and_run_i, crashes_temp_3a1dee.damage, crashes_temp_3a1dee.date_police_notified, crashes_temp_3a1dee.prim_contributory_cause, crashes_temp_3a1dee.sec_contributory_cause, crashes_temp_3a1dee.street_no, crashes_temp_3a1dee.street_direction, crashes_temp_3a1dee.street_name, crashes_temp_3a1dee.beat_of_occurrence, crashes_temp_3a1dee.photos_taken_i, crashes_temp_3a1dee.statements_taken_i, crashes_temp_3a1dee.dooring_i, crashes_temp_3a1dee.work_zone_i, crashes_temp_3a1dee.work_zone_type, crashes_temp_3a1dee.workers_present_i, crashes_temp_3a1dee.num_units, crashes_temp_3a1dee.most_severe_injury, crashes_temp_3a1dee.injuries_total, crashes_temp_3a1dee.injuries_fatal, crashes_temp_3a1dee.injuries_incapacitating, crashes_temp_3a1dee.injuries_non_incapacitating, crashes_temp_3a1dee.injuries_reported_not_evident, crashes_temp_3a1dee.injuries_no_indication, crashes_temp_3a1dee.injuries_unknown, crashes_temp_3a1dee.crash_hour, crashes_temp_3a1dee.crash_day_of_week, crashes_temp_3a1dee.crash_month, crashes_temp_3a1dee.latitude, crashes_temp_3a1dee.longitude); DROP TABLE public.crashes_temp_3a1dee; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-369329160416-us-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.crashes_temp_3a1dee",
        "connectionName": "redshift_connector",
        "preactions": "CREATE TABLE IF NOT EXISTS public.crashes (crash_record_id VARCHAR, crash_date VARCHAR, posted_speed_limit BIGINT, traffic_control_device VARCHAR, device_condition VARCHAR, weather_condition VARCHAR, lighting_condition VARCHAR, first_crash_type VARCHAR, trafficway_type VARCHAR, lane_cnt DOUBLE PRECISION, alignment VARCHAR, roadway_surface_cond VARCHAR, road_defect VARCHAR, report_type VARCHAR, crash_type VARCHAR, intersection_related_i VARCHAR, not_right_of_way_i VARCHAR, hit_and_run_i VARCHAR, damage VARCHAR, date_police_notified VARCHAR, prim_contributory_cause VARCHAR, sec_contributory_cause VARCHAR, street_no BIGINT, street_direction VARCHAR, street_name VARCHAR, beat_of_occurrence DOUBLE PRECISION, photos_taken_i VARCHAR, statements_taken_i VARCHAR, dooring_i VARCHAR, work_zone_i VARCHAR, work_zone_type VARCHAR, workers_present_i VARCHAR, num_units BIGINT, most_severe_injury VARCHAR, injuries_total DOUBLE PRECISION, injuries_fatal DOUBLE PRECISION, injuries_incapacitating DOUBLE PRECISION, injuries_non_incapacitating DOUBLE PRECISION, injuries_reported_not_evident DOUBLE PRECISION, injuries_no_indication DOUBLE PRECISION, injuries_unknown DOUBLE PRECISION, crash_hour BIGINT, crash_day_of_week BIGINT, crash_month BIGINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION); DROP TABLE IF EXISTS public.crashes_temp_3a1dee; CREATE TABLE public.crashes_temp_3a1dee (crash_record_id VARCHAR, crash_date VARCHAR, posted_speed_limit BIGINT, traffic_control_device VARCHAR, device_condition VARCHAR, weather_condition VARCHAR, lighting_condition VARCHAR, first_crash_type VARCHAR, trafficway_type VARCHAR, lane_cnt DOUBLE PRECISION, alignment VARCHAR, roadway_surface_cond VARCHAR, road_defect VARCHAR, report_type VARCHAR, crash_type VARCHAR, intersection_related_i VARCHAR, not_right_of_way_i VARCHAR, hit_and_run_i VARCHAR, damage VARCHAR, date_police_notified VARCHAR, prim_contributory_cause VARCHAR, sec_contributory_cause VARCHAR, street_no BIGINT, street_direction VARCHAR, street_name VARCHAR, beat_of_occurrence DOUBLE PRECISION, photos_taken_i VARCHAR, statements_taken_i VARCHAR, dooring_i VARCHAR, work_zone_i VARCHAR, work_zone_type VARCHAR, workers_present_i VARCHAR, num_units BIGINT, most_severe_injury VARCHAR, injuries_total DOUBLE PRECISION, injuries_fatal DOUBLE PRECISION, injuries_incapacitating DOUBLE PRECISION, injuries_non_incapacitating DOUBLE PRECISION, injuries_reported_not_evident DOUBLE PRECISION, injuries_no_indication DOUBLE PRECISION, injuries_unknown DOUBLE PRECISION, crash_hour BIGINT, crash_day_of_week BIGINT, crash_month BIGINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);",
    },
    transformation_ctx="AmazonRedshift_node1701491591925",
)

job.commit()
