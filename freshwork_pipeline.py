import http.client
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import requests
import json
from datetime import datetime
import pytz
import pandas as pd
from datetime import date
import requests

maild = [{'email': 'karmadeepsinh@tatvic.com'}, {'email': 'jaival@tatvic.com'}, {'email': 'yash.ch@tatvic.com'}, {'email': 'tushartyagi@thb.co.in'}]

def demo():
  tz_asia=pytz.timezone('Asia/Kolkata')

  currentDate= datetime.now(tz_asia).date()
  Day=currentDate.strftime("%d")
  Month=currentDate.strftime("%m")
  Year=currentDate.strftime("%Y") 

  conn = http.client.HTTPSConnection("agilus-help.freshdesk.com")
  payload = ''
  headers = {
    'Authorization': 'Basic SlEwcERNQUxoRGZORW03MVVxdm46',
    'Cookie': '_helpkit_session=BAh7BkkiD3Nlc3Npb25faWQGOgZFVEkiJWQwZjViOWRhMjE2MTlmMjZmZDBmZjcyNDdhZDM3NjFlBjsAVA%3D%3D--54f63ea3f4e084921259c44b0bac57e81d6362b5; _x_w=7_1; helpdesk_node_session=ff51af0af08e92aac7a1a3c6515b7301e0ad49ac35d5c09d2f789b3f264a031a3ce68e2bdd09e333e93c1451a112ec9e0079ec7d10ccc01b50a8c6c302770291'
  }
  conn.request("GET", "/reports/schedule/download_file.json?uuid=b28b55f9-995d-414d-8c23-e6a95eabead6", payload, headers)
  res = conn.getresponse()
  data = res.read()

  curl_url = data.decode("utf-8")
  csv_url = json.loads(curl_url)

  # Download CSV from URL
  response = requests.get(csv_url['export']['url'])

  if response.status_code == 200:

      # setting gcs bucket
      gcs_bucket_name = "assignment_jaival"
      gcs_bucket_folder_name = f"{Year}/{Month}/{Day}"
      # gcs_bucket_folder_name = "2023/12/22"
      file_name = currentDate
      # file_name = "2023-12-22"

      # Upload the CSV file to GCS
      client_storage = storage.Client(project="tatvic-gcp-dev-team")
      bucket = client_storage.bucket(gcs_bucket_name)
      blob = bucket.blob(f"{gcs_bucket_folder_name}/{file_name}.csv")
      file_content = StringIO(response.text)
      blob.upload_from_file(file_content, content_type='text/csv')

      # setting gcp bq
      dataset_name = "freshwork_test"
      table_name = "freshwork_table"
  
      # Move file from gcs to bq
      client_bq = bigquery.Client(project="tatvic-gcp-dev-team")
      table_ref = client_bq.dataset(dataset_id=dataset_name).table(table_id=table_name)

      # bq table configration

      schema = [
        bigquery.SchemaField(name="Ticket_ID",field_type="STRING"),
        bigquery.SchemaField(name="Agent_name",field_type="STRING"),
        bigquery.SchemaField(name="Status",field_type="STRING"),
        bigquery.SchemaField(name="Priority",field_type="STRING"),
        bigquery.SchemaField(name="Source",field_type="STRING"),
        bigquery.SchemaField(name="Customer_reply_count",field_type="STRING"),
        bigquery.SchemaField(name="Lab_Location__1",field_type="STRING"),
        bigquery.SchemaField(name="Query",field_type="STRING"),
        bigquery.SchemaField(name="First_response_time_in_calendar_hours",field_type="STRING"),
        bigquery.SchemaField(name="Route_Cause___2",field_type="STRING"),
        bigquery.SchemaField(name="Zone",field_type="STRING"),
        bigquery.SchemaField(name="Service_Recovery_Success",field_type="STRING"),
        bigquery.SchemaField(name="Accession_Type",field_type="STRING"),
        bigquery.SchemaField(name="Department__Assigned_1",field_type="STRING"),
        bigquery.SchemaField(name="Amount",field_type="STRING"),
        bigquery.SchemaField(name="Patient_Contact_Number",field_type="STRING"),
        bigquery.SchemaField(name="Sub_Type_of_Complaint_1",field_type="STRING"),
        bigquery.SchemaField(name="Mode_of_Complaint",field_type="STRING"),
        bigquery.SchemaField(name="Closed_date",field_type="STRING"),
        bigquery.SchemaField(name="Nature_Of_Complaint_1",field_type="STRING"),
        bigquery.SchemaField(name="Department__Assigned___2",field_type="STRING"),
        bigquery.SchemaField(name="First_assign_time_in_calendar_hours",field_type="STRING"),
        bigquery.SchemaField(name="Sub_Query",field_type="STRING"),
        bigquery.SchemaField(name="Subject",field_type="STRING"),
        bigquery.SchemaField(name="Unit_Name",field_type="STRING"),
        bigquery.SchemaField(name="Route_Cause_1",field_type="STRING"),
        bigquery.SchemaField(name="Sub_Type_of_Complaint_2",field_type="STRING"),
        bigquery.SchemaField(name="Attempt",field_type="STRING"),
        bigquery.SchemaField(name="Resolution_due_by",field_type="STRING"),
        bigquery.SchemaField(name="Validation",field_type="STRING"),
        bigquery.SchemaField(name="Last_updated_date",field_type="STRING"),
        bigquery.SchemaField(name="Customer_Type",field_type="STRING"),
        bigquery.SchemaField(name="Performing_Location_1",field_type="STRING"),
        bigquery.SchemaField(name="Unit_City",field_type="STRING"),
        bigquery.SchemaField(name="Created_date",field_type="STRING"),
        bigquery.SchemaField(name="Performing_Location_2",field_type="STRING"),
        bigquery.SchemaField(name="Nature_Of_Complaint_Subtype__1",field_type="STRING"),
        bigquery.SchemaField(name="Nature_Of_Complaint_2",field_type="STRING"),
        bigquery.SchemaField(name="City",field_type="STRING"),
        bigquery.SchemaField(name="Client_Type",field_type="STRING"),
        bigquery.SchemaField(name="Registered_Contact_Number",field_type="STRING"),
        bigquery.SchemaField(name="Resolution_time_in_calendar_hours",field_type="STRING"),
        bigquery.SchemaField(name="Registration_Location",field_type="STRING"),
        bigquery.SchemaField(name="Lab_Location___2",field_type="STRING"),
        bigquery.SchemaField(name="Requester_email",field_type="STRING"),
        bigquery.SchemaField(name="Group_name",field_type="STRING"),
        bigquery.SchemaField(name="Accession_Date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Acceptance_Date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Accession_No",field_type="STRING"),
        bigquery.SchemaField(name="Patient_Name",field_type="STRING"),
        bigquery.SchemaField(name="Client_Code",field_type="STRING"),
        bigquery.SchemaField(name="Client_Name",field_type="STRING"),
        bigquery.SchemaField(name="Attempt_Remark",field_type="STRING"),
        bigquery.SchemaField(name="Order_ID___TRF_No_",field_type="STRING"),
        bigquery.SchemaField(name="Phlebotomist_Name",field_type="STRING"),
        bigquery.SchemaField(name="VOC__Voice_Of_Customer_",field_type="STRING"),
        bigquery.SchemaField(name="Investigator_Comment",field_type="STRING"),
        bigquery.SchemaField(name="Corrective_Active__CA___1",field_type="STRING"),
        bigquery.SchemaField(name="Action_Taken",field_type="STRING"),
        bigquery.SchemaField(name="Corrective_Active__CA____2",field_type="STRING"),
        bigquery.SchemaField(name="Preventive_Action__PA____1",field_type="STRING"),
        bigquery.SchemaField(name="Preventive_Action__PA____2",field_type="STRING"),
        bigquery.SchemaField(name="Attempt_1_date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Attempt_2_date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Attempt_3_date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Resolved_Date___Time",field_type="STRING"),
        bigquery.SchemaField(name="Created_By",field_type="STRING"),
      ]
      job_config = bigquery.LoadJobConfig()
      job_config.source_format = bigquery.SourceFormat.CSV
      job_config.skip_leading_rows = 1
      job_config.schema = schema
      job_config.max_bad_records = 100000
      job_config.encoding = "UTF-8" 

      # loading file into dataframe
      gcs_uri = f"gs://{gcs_bucket_name}/{gcs_bucket_folder_name}/{file_name}.csv"

      try:
          load_job = client_bq.load_table_from_uri(source_uris=gcs_uri,destination=table_ref,job_config=job_config)
          load_job.result()
          print(f"Data appended to BigQuery table successfully for date {currentDate}.")
      except Exception as e:
          curr_date = date.today().strftime("%Y%m%d")
          email_content = (
              '<html><head></head><body>Hi Team,<br><br>'
              '<font color="red">Freshworks Data is not appended to BigQuery table  for date :</font> '
              f'{", ".join(curr_date)}<br><br></body></html>'
          )

          data = {
              'from': {'name': 'CloudId1 Tatvic', 'email': 'cloud.id1@tatvic.com'},
              'to': maild,
              'replyTo': [{'name': 'CloudId1 Tatvi Tatvic', 'email': 'cloud.id1@tatvic.com'}],
              'subject': '[Freshworks] Unable to Load table into BQ',
              'html': email_content
          }

          url = 'https://mailapi.tatvic.com/send-message'
          headers = {'key': ''}
          response = requests.post(url, json=data, headers=headers)

          if response.status_code == 200:
              print("Alert Mail Sent")
          else:
              print(f"Failed to send alert email. Status code: {response.status_code}")
        
      destination_table = client_bq.get_table(table_name=table_name)
      print(f"Loaded {destination_table} rows")

if __name__ == "__main__":
    demo()


