import requests
import json
import schemas
from sqlalchemy.orm import Session
from fastapi import Depends ,FastAPI
from db import SessionLocal


app = FastAPI()

base_url = 'https://api.icmr.org.in/covid_data/index.php/login'
base_url_2 = 'https://api.icmr.org.in/covid_data/index.php/covid-data'

token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjI0IiwidXNlcm5hbWUiOiJyYW5qZWV0LnNpbmdoLm1hc2hvbiIsInNlY19rZXkiOiJtYXNob25AY21jbHVkaGlhbmEuaW4iLCJpYXQiOjE2NjEzMzQwMzgsImV4cCI6MTY2MTMzNDkzOH0.TDlLRNb00IOZlZvhHROqTmxfrwwTlO9Z2F3Oew_q_GA'

headers = {'Authorization': "Bearer {}".format(token)}

data = {"username": "ranjeet.singh.mashon",
        "password": "ranjeet.singh^&$@icmr1239"}

# data_duration = {"date_from":"2022-07-04 00:00:00", 
#                 "date_to":"2022-07-04 10:00:00",
#                 "offset":"10000"}

def get_db():
    db = None
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()

@app.post('/icmr')
async def save_into_db (db: Session = Depends(get_db)):
        offset = db.query(schemas.IcmrCovidDetails.user_id).order_by(schemas.IcmrCovidDetails.user_id.desc()).first()
        str_offset = ''.join(str(offset))[1:-2] 
        print(str_offset)
        if str_offset:
                icmr_data_request = requests.post(base_url_2,data= json.dumps({"date_from":"2020-01-01 00:00:00", 
                        "date_to":"2022-08-23 10:00:00",
                        "offset":f'{str_offset}'}), headers=headers)
        else:
                icmr_data_request = requests.post(base_url_2,data= json.dumps({"date_from":"2020-01-01 00:00:00", 
                        "date_to":"2022-08-23 10:00:00"}), headers=headers)

        icmr_response = icmr_data_request.json()
        # print(icmr_response)
        # icmr_data_request.raise_for_status()
        try:
                for i in icmr_response.get('response')['data_result']:
                        icmr_data = schemas.IcmrCovidDetails()
                        icmr_data.icmr_id = i['icmr_id']
                        icmr_data.clinical_id =  i['clinical_id']
                        icmr_data.age =  i['age']
                        icmr_data.age_in =  i['age_in']
                        icmr_data.gender =  i['gender']
                        icmr_data.patient_category =  i['patient_category']
                        icmr_data.state_residence =  i['state_residence']
                        icmr_data.district_residence =  i['district_residence']
                        icmr_data.lab_id =  i['lab_id']
                        icmr_data.date_sample_collection =  i['date_sample_collection']
                        icmr_data.date_sample_received =  i['date_sample_received']
                        icmr_data.date_of_sample_tested =  i['date_of_sample_tested']
                        icmr_data.symptoms =  i['symptoms']
                        icmr_data.underlying_medical_condition =  i['underlying_medical_condition']
                        icmr_data.testing_kit_used =  i['testing_kit_used']
                        icmr_data.egene_screening =  i['egene_screening']
                        icmr_data.ct_value_screening =  i['ct_value_screening']
                        icmr_data.rdrp_confirmatory =  i['rdrp_confirmatory']
                        icmr_data.ct_value_rdrp =  i['ct_value_rdrp']
                        icmr_data.orf1b_confirmatory =  i['orf1b_confirmatory']
                        icmr_data.ct_value_orf1b =  i['ct_value_orf1b']
                        icmr_data.final_test_result =  i['final_test_result']
                        icmr_data.entry_date =  i['entry_date']
                        icmr_data.updated_on =  i['updated_on']

                        db.add(icmr_data)
                        db.commit()
                print(icmr_data_request.status_code)
                await save_into_db()
        except Exception as e:
                return e


@app.get('/icmr/details')
async def get_from_db (db: Session = Depends(get_db)):
        result = db.query(schemas.IcmrCovidDetails.user_id).order_by(schemas.IcmrCovidDetails.user_id.desc()).first()
        if result:
                x =''.join(str(result))
                print(type(x))
                return result
        else :
                return {'messgae': 'unable to fetch data'}




@app.get('/userid')
def user_with_id(db : Session = Depends(get_db)):
    
    result = db.query(schemas.IcmrCovidDetails).filter(schemas.IcmrCovidDetails.icmr_id=='Kvuku4XIG0Ev8oum0vaXijKHM8CepIsR8ZnpSi+VjnU=').all()
    return result




