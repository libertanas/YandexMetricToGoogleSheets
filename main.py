import os

from datetime import date
from dateutil.relativedelta import relativedelta
from gspread import service_account

from api_util import get_log_data
from api_fields import hit_field_list, visit_field_list


if __name__ == "__main__":
    
    token = os.getenv("TOKEN")
    counter_id = os.getenv("COUNTER_ID")
    api_host_url = "https://api-metrika.yandex.ru"

    MAX_ROWS = 200_000
    
    FILTER_BRANCHES = [
    #"https://i.transport.mos.ru/perekrytiya/",
    "https://i.transport.mos.ru/spasibo/"
    ]
    
    start_date = os.getenv("START_DATE")
    if not start_date or not start_date.strip():
        start_date = (date.today() - relativedelta(days=2)).strftime("%Y-%m-%d")
   
    end_date = os.getenv("END_DATE")
    if not end_date or not end_date.strip():
        yesterday = (date.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        end_date = yesterday

    data_list = [{
        "source": "hits",
        "api_field_list": hit_field_list,
        "google_sheet_url": os.getenv("HIT_SHEET_URL")
    }#, {
     #   "source": "visits",
     #   "api_field_list": visit_field_list,
     #   "google_sheet_url": os.getenv("VISIT_SHEET_URL")
    #}
     ]
    gc = service_account()

    for data_elem in data_list:
        data = get_log_data(api_host_url,
                            counter_id,
                            token,
                            data_elem["source"],
                            start_date,
                            end_date,
                            data_elem["api_field_list"])
        
        print("START_DATE:", start_date)
        print("END_DATE:", end_date)
        print("Rows downloaded:", len(data))
        print("Columns:", data.columns.tolist())

        if data_elem["source"] == "hits":
            print("isPageView values:")
            print(data["ym:pv:isPageView"].value_counts(dropna=False))

            print("URLs sample:")
            print(data["ym:pv:URL"].dropna().head(20).tolist())

            data = data[
                data["ym:pv:URL"].str.contains("/spasibo/", na=False)
                & (data["ym:pv:isPageView"].astype(str) == "1")
            ]
        
            print("Rows after filter:", len(data))
            
            if len(data) > 0:
                print("Dates after filter:", data["ym:pv:date"].min(), data["ym:pv:date"].max())
                print("Filtered URLs sample:")
                print(data["ym:pv:URL"].dropna().head(20).tolist())
        
        if len(data) > MAX_ROWS:
            data = data.tail(MAX_ROWS)
    
        sh = gc.open_by_url(data_elem["google_sheet_url"])
        worksheet = sh.worksheet("hits") #if data_elem["source"] == "hits" #else sh.worksheet("visits")
        
        worksheet.update(
            [data.columns.values.tolist()] + data.fillna("Unknown").values.tolist()
        )
        
