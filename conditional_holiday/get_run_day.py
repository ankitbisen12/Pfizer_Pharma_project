from datetime import datetime

current_date = datetime.now().strftime("%Y-%m-%d")

dbutils.jobs.taskValues.set(
    key="input_date",
    value=current_date)