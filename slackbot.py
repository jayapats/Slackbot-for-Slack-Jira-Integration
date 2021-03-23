import snowflake.connector
import pandas as pd
from pandas import DataFrame
import slack
from jira import JIRA
from tabulate import tabulate
import config
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
con = snowflake.connector.connect(
    user=config.snowflake_user,
    password=config.snowflake_password,
    account='GRA')
shuttle_query="""
select to_date(clock_in) as SHUTTLE_RUN_DATE,clock_in_by_login AS SHUTTLE_NAME, count(*) AS TOTAL_SCANS from HACKATHON_IT.ANALYTICS.SHUTTLE_SCANS
group by to_date(clock_in),clock_in_by_login
order by clock_in_by_login,to_date(clock_in)
;
"""
p6_scans_query="""select to_date(clock_in) as SHUTTLE_RUN_DATE,count(*) as COUNT_OF_SCANS
from HACKATHON_IT.ANALYTICS.SHUTTLE_SCANS where clock_in_by_login='p6' and (to_time(clock_in) between '09:30:00' and '11:00:00')
group by to_date(clock_in)
order by to_date(clock_in)
;"""
new_hires_query="""select employee_name,employee_id,computer_type,keyboard_mouse,number_of_monitors,required_systems,email_home from HACKATHON_IT.ANALYTICS.NEW_HIRES limit 1;"""
p6_scans_data=pd.read_sql_query(p6_scans_query,con)
df_p6_scans=DataFrame(p6_scans_data)
shuttle_data=pd.read_sql_query(shuttle_query,con)
df_shuttle_data=DataFrame(shuttle_data)
# print(df_shuttle_data)df_shuttle_data
print(tabulate(shuttle_data))
# new_hire_data=pd.read_sql_query(new_hires_query,con)
# print(new_hire_data)
slack_filename='shuttle_report.xlsx'
writer = pd.ExcelWriter(slack_filename, engine='xlsxwriter')
df_shuttle_data.to_excel(writer,sheet_name='Scans by shuttles',index=False)
df_p6_scans.to_excel(writer,sheet_name='P6 Scans 9-30 to 11-00 AM',index=False)
writer.save()
client = slack.WebClient(token=config.slack_api_token) #move this to config
#file upload as an excel file with multiple tabs containing data
response = client.files_upload(
    channels='#hackathon,UP4LR6PNW',
    file=slack_filename,
    filename=slack_filename,
    initial_comment="shuttle report"
)
assert response["ok"]
snippet_headers = ['SHUTTLE_RUN_DATE','SHUTTLE_NAME','TOTAL_SCANS']
#data from table sent as a code snippet in tabular format. This looks better and is  preferred.
response2 = client.files_upload(
    channels='#hackathon,UP4LR6PNW',
    content=tabulate(shuttle_data, headers=snippet_headers, showindex=False,tablefmt="psql"), #tablefmt options grid, psql, jira, fancy_grid
    initial_comment="shuttle report as snippet"
)
assert response2["ok"]
# data from table sent as a message
response3 = client.chat_postMessage(
    channel='#hackathon',
    mrkdwn = True,
    text = str(df_shuttle_data)
)
assert response3["ok"]
#below code collects new hire data and creates jira tickets based on the data
new_hire_data2=con.cursor().execute(new_hires_query)
for row in new_hire_data2:
    # print(row)
    employee_name = row[0]
    employee_id =row[1]
    computer_type=row[2]
    keyboard_mouse=row[3]
    number_of_monitors=row[4]
    req_systems=row[5]
    email=row[6]
options = {'server': 'https://datadiggers.atlassian.net'}
username = config.jira_username
password = config.jira_password
jira = JIRA(options, basic_auth=(username, password))
# print(employee_id,employee_name,req_systems)
issue_list = [
{
    'project': {'key': 'AS'}, #This is for the asset team
    'summary': 'Provision a assets for '+employee_name+'',
    'description': computer_type+', '+keyboard_mouse+ ', ' + number_of_monitors,
    'issuetype': {'name': 'Task'}
},
{
    'project': {'key': 'IS'}, #This is for the IT support team
    'summary': 'Provide access to systems for '+employee_name,
    'description': req_systems,
    'issuetype': {'name': 'Task'}
},
{
    'project': {'key': 'DAT'}, #This is for the Engg team
    'summary': 'Create a snowflake user account for '+employee_name,
    'description': email,
    'issuetype': {'name': 'Task'},
    'assignee': {'name': 'admin'} #get this from a populated ticket
}]
issues = jira.create_issues(field_list=issue_list)