import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
import gspread
from gspread_dataframe import set_with_dataframe

timer = time.time()
payload = [
    'Number of deaths',
    'Number of infant deaths',
    'Number of under-five deaths',
    'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)',
    'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)',
    'Estimates of number of homicides',
    'Crude suicide rates (per 100 000 population)',
    'Mortality rate attributed to unintentional poisoning (per 100 000 population)',
    'Number of deaths attributed to non-communicable diseases, by type of disease and sex',
    'Estimated road traffic death rate (per 100 000 population)',
    'Estimated number of road traffic deaths',
    'Mean BMI (crude estimate)',
    'Mean BMI (age-standardized estimate)',
    'Prevalence of obesity among adults, BMI > 30 (age-standardized estimate) (%)',
    'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)',
    'Prevalence of overweight among adults, BMI > 25 (age-standardized estimate) (%)',
    'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)',
    'Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%),',
    'Prevalence of thinness among children and adolescents, BMI < -2 standarddeviations below the median (crude estimate) (%)',
    'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)',
    'Estimate of daily cigarette smoking prevalence (%)',
    'Estimate of daily tobacco smoking prevalence (%)',
    'Estimate of current cigarette smoking prevalence (%)',
    'Estimate of current tobacco smoking prevalence (%)',
    'Mean systolic blood pressure (crude estimate)',
    'Mean fasting blood glucose (mmol/l) (crude estimate)',
    'Mean Total Cholesterol (crude estimate)'
]

def removeFacts(rows, root):
    events = ET.iterparse(root.raw)
    for event, elem in events:
        try:
            if elem.tag == 'AGEGROUP':
                ageGroup = elem.text if elem.text else 'Null'
            if elem.tag == 'COUNTRY':
                country = elem.text if elem.text else 'Null'
            if elem.tag == 'GHECAUSES':
                gheCauses = elem.text if elem.text else 'Null'
            if elem.tag == 'GHO' and elem.text in payload:
                data = elem.text if elem.text else 'Null'
            if elem.tag == 'YEAR':
                year = elem.text if elem.text else 'Null'
            if elem.tag == 'Numeric':
                numeric = float(elem.text) if elem.text else 0
            if elem.tag == 'SEX':
                sex = elem.text if elem.text else 'Null'
            if elem.tag == 'Display':
                display = elem.text if elem.text else 'Null'
            if elem.tag == 'Low':
                low = float(elem.text) if elem.text else 0
            if elem.tag == 'High':
                high = float(elem.text) if elem.text else 0
            if elem.tag == 'Fact' and event == 'end':
                rows.append({
                    'country': country,
                    'year': year,
                    'data': data,
                    'numeric': numeric,
                    'sex': sex,
                    'gheCauses': gheCauses,
                    'ageGroup': ageGroup,
                    'display': display,
                    'low': low,
                    'high': high,
                })
                var = False
        except:
            continue
    return rows

r1 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_USA.xml', stream=True)
r2 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_JPN.xml', stream=True)
r3 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHL.xml', stream=True)
r4 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ESP.xml', stream=True)
r5 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_EGY.xml', stream=True)
r6 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_AUS.xml', stream=True)

df_cols = ['country', 'year', 'data', 'numeric', 'sex', 'gheCauses', 'ageGroup', 'display', 'low', 'high']
df_rows = []

df_rows = removeFacts(df_rows, r1)
df_rows = removeFacts(df_rows, r2)
df_rows = removeFacts(df_rows, r3)
df_rows = removeFacts(df_rows, r4)
df_rows = removeFacts(df_rows, r5)
df_rows = removeFacts(df_rows, r6)
df = pd.DataFrame(df_rows, columns=df_cols)

print('1.1', (time.time() - timer) / 60)
print(df)


gc = gspread.service_account(filename='tarea4.json')
sh = gc.open_by_key('1YBS-IgOjqn5sDC77GJLdqDWZC2KLqCuNyo7zA1kV94M')
worksheet = sh.get_worksheet(0)

set_with_dataframe(worksheet, df)
